/* auth.js — CloudFront 部署版的登录门禁与日志加载
 *
 * 职责:
 *   1) 在页面上覆盖一个登录界面(登录 / 首次登录改密 / 忘记密码)。
 *   2) 通过 Amazon Cognito User Pool 完成认证(纯 REST 调用，无需签名):
 *        - InitiateAuth (USER_PASSWORD_AUTH)
 *        - RespondToAuthChallenge (NEW_PASSWORD_REQUIRED)
 *        - ForgotPassword / ConfirmForgotPassword
 *   3) 认证成功后把 IdToken 放入 Authorization header，
 *      通过 API Gateway 查询 DynamoDB（不再使用 Identity Pool / 临时凭证）。
 *      - GET /instances           → 获取当前用户有权访问的实例列表
 *      - GET /contacts            → 按 instanceName 加载 Contact 摘要列表
 *      - GET /contacts/{id}/events → 按需加载单个 Contact 事件明细
 *   4) 登录后右上角显示实例切换器，允许用户切换到其他有权限的实例。
 *   5) IdToken 存入 sessionStorage，刷新页面自动恢复；关闭标签页自动清除。
 *   6) Header 右上角显示退出按钮，点击清除 session 并返回登录页。
 *
 * 依赖: aws-config.js(部署脚本生成，含各资源 ID 和 apiUrl)。
 */
(function () {
  "use strict";

  var CFG = window.__AWS_CONFIG__ || {};
  var IDP_ENDPOINT = "https://cognito-idp." + CFG.region + ".amazonaws.com/";
  var idToken = "";
  var pendingEmail = "";
  var pendingSession = "";
  var currentInstance = "";   // 当前选中的实例名
  var allowedInstances = [];  // 当前用户有权访问的实例列表
  var instanceLabels = {};    // instanceName -> 展示文本

  // ---- sessionStorage 持久化登录态（关闭标签页自动清除）----
  var SK_TOKEN     = "auth_id_token";
  var SK_EXPIRES   = "auth_expires_at";   // IdToken 过期时间，Unix 秒
  var SK_REFRESH   = "auth_refresh_token";
  var SK_LOGIN_TS  = "auth_login_ts";     // 首次登录时间，Unix 秒（用于 24h 强制重新登录）

  var MAX_SESSION_SECONDS = 24 * 3600;    // 最大会话时长 24 小时

  function saveSession(token, expiresIn, refreshToken) {
    var now = Math.floor(Date.now() / 1000);
    var expiresAt = now + (expiresIn || 3600);
    sessionStorage.setItem(SK_TOKEN,    token);
    sessionStorage.setItem(SK_EXPIRES,  String(expiresAt));
    if (refreshToken) {
      sessionStorage.setItem(SK_REFRESH, refreshToken);
    }
    // 仅在首次登录时记录 login_ts（刷新 token 时不更新）
    if (!sessionStorage.getItem(SK_LOGIN_TS)) {
      sessionStorage.setItem(SK_LOGIN_TS, String(now));
    }
  }

  function loadSession() {
    var token = sessionStorage.getItem(SK_TOKEN);
    var exp   = parseInt(sessionStorage.getItem(SK_EXPIRES) || "0", 10);
    var loginTs = parseInt(sessionStorage.getItem(SK_LOGIN_TS) || "0", 10);
    if (!token) return null;

    // 检查 24h 最大会话时长：超过则强制清除
    var now = Math.floor(Date.now() / 1000);
    if (loginTs && (now - loginTs) >= MAX_SESSION_SECONDS) {
      clearSession();
      return null;
    }

    // IdToken 已过期但有 RefreshToken -> 返回 "expired" 让调用方触发刷新
    if (exp && now >= exp - 60) {
      var rt = sessionStorage.getItem(SK_REFRESH);
      if (rt) return "expired";
      clearSession();
      return null;
    }
    return token;
  }

  function clearSession() {
    sessionStorage.removeItem(SK_TOKEN);
    sessionStorage.removeItem(SK_EXPIRES);
    sessionStorage.removeItem(SK_REFRESH);
    sessionStorage.removeItem(SK_LOGIN_TS);
  }

  // ---- 用 RefreshToken 静默刷新 IdToken ----
  function refreshIdToken() {
    var rt = sessionStorage.getItem(SK_REFRESH);
    if (!rt) return Promise.reject(new Error("no refresh token"));

    var loginTs = parseInt(sessionStorage.getItem(SK_LOGIN_TS) || "0", 10);
    var now = Math.floor(Date.now() / 1000);
    // 超过 24h 不再刷新，强制重新登录
    if (loginTs && (now - loginTs) >= MAX_SESSION_SECONDS) {
      clearSession();
      return Promise.reject(new Error("session expired (24h)"));
    }

    return idp("InitiateAuth", {
      AuthFlow: "REFRESH_TOKEN_AUTH",
      ClientId: CFG.clientId,
      AuthParameters: { REFRESH_TOKEN: rt },
    }).then(function (data) {
      if (data.AuthenticationResult && data.AuthenticationResult.IdToken) {
        var newToken = data.AuthenticationResult.IdToken;
        var newExpiresIn = data.AuthenticationResult.ExpiresIn || 3600;
        // RefreshToken 通常不随刷新返回，保留已有的
        saveSession(newToken, newExpiresIn, null);
        idToken = newToken;
        return newToken;
      }
      throw new Error("refresh failed: no IdToken in response");
    }).catch(function (e) {
      // 刷新失败 -> 清除 session，下次操作时会回到登录页
      clearSession();
      throw e;
    });
  }

  // ---- API Gateway 调用（带 IdToken 鉴权，401 时自动刷新重试一次）----
  function apiGet(path, params) {
    var url = (CFG.apiUrl || "") + path;
    if (params && Object.keys(params).length) {
      var qs = Object.keys(params)
        .map(function (k) { return encodeURIComponent(k) + "=" + encodeURIComponent(params[k]); })
        .join("&");
      url += "?" + qs;
    }
    function doFetch() {
      return fetch(url, {
        method: "GET",
        headers: {
          "Authorization": idToken,
          "Content-Type": "application/json",
        },
      }).then(function (res) {
        return res.json().then(function (data) {
          if (!res.ok) {
            var err = new Error(data.error || data.message || ("HTTP " + res.status));
            err.status = res.status;
            throw err;
          }
          return data;
        });
      });
    }
    return doFetch().catch(function (err) {
      // 401/403 且有 RefreshToken -> 刷新后重试一次
      if ((err.status === 401 || err.status === 403) && sessionStorage.getItem(SK_REFRESH)) {
        return refreshIdToken().then(function () { return doFetch(); });
      }
      throw err;
    });
  }

  // ---- 简单的双语文案 ----
  var T = {
    title:        "Connect AI Agent 日志排查",
    subtitle:     "请登录后查看会话日志 / Sign in to view logs",
    email:        "邮箱 Email",
    password:     "密码 Password",
    login:        "登录 Sign in",
    forgot:       "忘记密码？Forgot password?",
    newPassTitle: "首次登录，请设置新密码 / Set a new password",
    newPass:      "新密码 New password",
    confirmPass:  "确认新密码 Confirm password",
    submit:       "提交 Submit",
    back:         "返回登录 Back",
    forgotTitle:  "忘记密码 / Reset password",
    sendCode:     "发送验证码 Send code",
    code:         "邮箱收到的验证码 Verification code",
    resetPass:    "重置密码 Reset password",
    loading:      "正在加载联系人摘要… Loading contacts…",
    signingIn:    "正在登录… Signing in…",
    codeSent:     "验证码已发送到你的邮箱，请查收。A code has been sent to your email.",
    pwMismatch:   "两次输入的密码不一致。Passwords do not match.",
    needConfig:   "缺少部署配置(aws-config.js)，无法登录。",
    switchInst:   "切换实例",
    noInstances:  "当前账号没有可访问的实例权限，请联系管理员。",
    signOut:      "退出",
  };

  // 优先从 window.I18N（由 i18n.js 提供）读取当前语言的翻译，回退到 T 内置值
  function tAuth(key) {
    // 从页面语言选择器读当前语言（app.js 初始化后才有值，默认中文）
    var sel = document.getElementById("langSelect");
    var lang = (sel && sel.value) ||
               (window.I18N_LANGS && window.I18N_LANGS[0]) || "中文";
    var dict = window.I18N && window.I18N[lang];
    if (dict && dict[key] != null) return dict[key];
    return T[key] || key;
  }

  // ---- Cognito IDP REST 调用 ----
  function idp(target, body) {
    return fetch(IDP_ENDPOINT, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-amz-json-1.1",
        "X-Amz-Target": "AWSCognitoIdentityProviderService." + target,
      },
      body: JSON.stringify(body),
    }).then(function (res) {
      return res.json().then(function (data) {
        if (!res.ok) {
          var msg = data.message || data.Message || data.__type || "请求失败";
          throw new Error(msg);
        }
        return data;
      });
    });
  }

  // ---- DOM 辅助 ----
  var overlay, statusEl;

  function h(tag, attrs, children) {
    var el = document.createElement(tag);
    attrs = attrs || {};
    for (var k in attrs) {
      if (k === "style") el.style.cssText = attrs[k];
      else if (k === "class") el.className = attrs[k];
      else el.setAttribute(k, attrs[k]);
    }
    (children || []).forEach(function (c) {
      el.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
    });
    return el;
  }

  function injectStyles() {
    var css =
      "#authOverlay{position:fixed;inset:0;z-index:99999;background:#0f1117;" +
      "display:flex;align-items:center;justify-content:center;" +
      "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'PingFang SC','Microsoft YaHei',sans-serif;color:#e6e8ee}" +
      "#authCard{width:380px;max-width:92vw;background:#171a22;border:1px solid #2a2f3a;" +
      "border-radius:12px;padding:26px 24px;box-shadow:0 12px 40px rgba(0,0,0,.5)}" +
      "#authCard h2{margin:0 0 4px;font-size:18px}" +
      "#authCard .sub{color:#9aa3b2;font-size:12px;margin-bottom:18px}" +
      "#authCard label{display:block;font-size:12px;color:#9aa3b2;margin:12px 0 5px}" +
      "#authCard input{width:100%;padding:10px 11px;background:#1e222c;border:1px solid #2a2f3a;" +
      "border-radius:7px;color:#e6e8ee;font-size:14px;box-sizing:border-box}" +
      "#authCard input:focus{outline:none;border-color:#4f9cff}" +
      "#authCard button.primary{width:100%;margin-top:18px;padding:11px;background:#4f9cff;" +
      "color:#fff;border:none;border-radius:7px;font-size:14px;font-weight:600;cursor:pointer}" +
      "#authCard button.primary:hover{background:#3d8bef}" +
      "#authCard button.primary:disabled{opacity:.6;cursor:default}" +
      "#authCard .linkrow{margin-top:14px;text-align:center}" +
      "#authCard a.link{color:#4f9cff;font-size:12px;cursor:pointer;text-decoration:none}" +
      "#authCard a.link:hover{text-decoration:underline}" +
      "#authStatus{margin-top:14px;font-size:12px;min-height:16px;text-align:center;" +
      "color:#9aa3b2;white-space:pre-wrap}" +
      "#authStatus.err{color:#f85149}" +
      "#authStatus.ok{color:#3fb950}" +
      /* 实例切换器（嵌在 header 里，不再 fixed 浮动） */
      "#instSwitcher{display:flex;align-items:center;gap:8px;margin-left:8px}" +
      "#instSwitcher .inst-label{font-size:12px;color:#9aa3b2;white-space:nowrap}" +
      "#instSwitcher select{padding:5px 8px;background:var(--panel-2,#1e222c);border:1px solid var(--border,#2a2f3a);" +
      "border-radius:6px;color:var(--text,#e6e8ee);font-size:12px;cursor:pointer}" +
      "#instSwitcher select:focus{outline:none;border-color:#4f9cff}" +
      /* 退出按钮 */
      "#btnSignOut{padding:5px 12px;background:transparent;border:1px solid #3a3f4a;" +
      "border-radius:6px;color:#9aa3b2;font-size:12px;cursor:pointer;margin-left:8px}" +
      "#btnSignOut:hover{border-color:#f85149;color:#f85149}";
    document.head.appendChild(h("style", {}, [css]));
  }

  function setStatus(msg, kind) {
    if (!statusEl) return;
    statusEl.textContent = msg || "";
    statusEl.className = kind || "";
  }

  function clearCard() {
    var card = document.getElementById("authCard");
    card.innerHTML = "";
    return card;
  }

  // ---- 视图: 登录 ----
  function showLogin(prefillEmail) {
    var card = clearCard();
    card.appendChild(h("h2", {}, [T.title]));
    card.appendChild(h("div", { class: "sub" }, [T.subtitle]));

    var emailInput = h("input", { type: "email", id: "emEmail",
      value: prefillEmail || pendingEmail || "", autocomplete: "username" });
    var passInput  = h("input", { type: "password", id: "emPass",
      autocomplete: "current-password" });

    card.appendChild(h("label", {}, [T.email]));
    card.appendChild(emailInput);
    card.appendChild(h("label", {}, [T.password]));
    card.appendChild(passInput);

    var btn     = h("button", { class: "primary" }, [T.login]);
    var linkRow = h("div", { class: "linkrow" }, []);
    var forgot  = h("a", { class: "link" }, [T.forgot]);
    linkRow.appendChild(forgot);
    card.appendChild(btn);
    card.appendChild(linkRow);

    statusEl = h("div", { id: "authStatus" }, []);
    card.appendChild(statusEl);

    function submit() {
      var email = emailInput.value.trim();
      var pass  = passInput.value;
      if (!email || !pass) { setStatus("请输入邮箱和密码。", "err"); return; }
      pendingEmail = email;
      btn.disabled = true;
      setStatus(T.signingIn, "");
      idp("InitiateAuth", {
        AuthFlow: "USER_PASSWORD_AUTH",
        ClientId: CFG.clientId,
        AuthParameters: { USERNAME: email, PASSWORD: pass },
      }).then(function (data) {
        if (data.ChallengeName === "NEW_PASSWORD_REQUIRED") {
          pendingSession = data.Session;
          showNewPassword();
          return;
        }
        if (data.AuthenticationResult && data.AuthenticationResult.IdToken) {
          // 保存 token 到 sessionStorage（含 RefreshToken）
          saveSession(
            data.AuthenticationResult.IdToken,
            data.AuthenticationResult.ExpiresIn,
            data.AuthenticationResult.RefreshToken
          );
          onAuthenticated(data.AuthenticationResult.IdToken);
          return;
        }
        setStatus("暂不支持的认证流程: " + (data.ChallengeName || "?"), "err");
        btn.disabled = false;
      }).catch(function (e) {
        setStatus(e.message || "登录失败", "err");
        btn.disabled = false;
      });
    }

    btn.addEventListener("click", submit);
    passInput.addEventListener("keydown", function (e) { if (e.key === "Enter") submit(); });
    forgot.addEventListener("click", function () { showForgot(emailInput.value.trim()); });
  }

  // ---- 视图: 首次登录改密 ----
  function showNewPassword() {
    var card = clearCard();
    card.appendChild(h("h2", {}, [T.newPassTitle]));
    card.appendChild(h("div", { class: "sub" }, [pendingEmail]));

    var p1 = h("input", { type: "password", id: "np1", autocomplete: "new-password" });
    var p2 = h("input", { type: "password", id: "np2", autocomplete: "new-password" });
    card.appendChild(h("label", {}, [T.newPass]));
    card.appendChild(p1);
    card.appendChild(h("label", {}, [T.confirmPass]));
    card.appendChild(p2);

    var btn = h("button", { class: "primary" }, [T.submit]);
    card.appendChild(btn);
    statusEl = h("div", { id: "authStatus" }, []);
    card.appendChild(statusEl);

    btn.addEventListener("click", function () {
      if (p1.value !== p2.value) { setStatus(T.pwMismatch, "err"); return; }
      if (!p1.value) { setStatus("请输入新密码。", "err"); return; }
      btn.disabled = true;
      setStatus(T.signingIn, "");
      idp("RespondToAuthChallenge", {
        ClientId: CFG.clientId,
        ChallengeName: "NEW_PASSWORD_REQUIRED",
        Session: pendingSession,
        ChallengeResponses: { USERNAME: pendingEmail, NEW_PASSWORD: p1.value },
      }).then(function (data) {
        if (data.AuthenticationResult && data.AuthenticationResult.IdToken) {
          // 保存 token 到 sessionStorage（含 RefreshToken）
          saveSession(
            data.AuthenticationResult.IdToken,
            data.AuthenticationResult.ExpiresIn,
            data.AuthenticationResult.RefreshToken
          );
          onAuthenticated(data.AuthenticationResult.IdToken);
        } else if (data.ChallengeName) {
          setStatus("暂不支持的后续挑战: " + data.ChallengeName, "err");
          btn.disabled = false;
        }
      }).catch(function (e) {
        setStatus(e.message || "设置新密码失败", "err");
        btn.disabled = false;
      });
    });
  }

  // ---- 视图: 忘记密码 ----
  function showForgot(prefillEmail) {
    var card = clearCard();
    card.appendChild(h("h2", {}, [T.forgotTitle]));
    card.appendChild(h("div", { class: "sub" }, [T.subtitle]));

    var emailInput = h("input", { type: "email",
      value: prefillEmail || pendingEmail || "", autocomplete: "username" });
    card.appendChild(h("label", {}, [T.email]));
    card.appendChild(emailInput);

    var sendBtn  = h("button", { class: "primary" }, [T.sendCode]);
    card.appendChild(sendBtn);

    var codeWrap = h("div", { style: "display:none" }, []);
    var codeInput = h("input", { type: "text", autocomplete: "one-time-code" });
    var np1       = h("input", { type: "password", autocomplete: "new-password" });
    codeWrap.appendChild(h("label", {}, [T.code]));
    codeWrap.appendChild(codeInput);
    codeWrap.appendChild(h("label", {}, [T.newPass]));
    codeWrap.appendChild(np1);
    var resetBtn = h("button", { class: "primary" }, [T.resetPass]);
    codeWrap.appendChild(resetBtn);
    card.appendChild(codeWrap);

    var linkRow = h("div", { class: "linkrow" }, []);
    var back = h("a", { class: "link" }, [T.back]);
    linkRow.appendChild(back);
    card.appendChild(linkRow);

    statusEl = h("div", { id: "authStatus" }, []);
    card.appendChild(statusEl);

    sendBtn.addEventListener("click", function () {
      var email = emailInput.value.trim();
      if (!email) { setStatus("请输入邮箱。", "err"); return; }
      pendingEmail = email;
      sendBtn.disabled = true;
      setStatus("发送中… Sending…", "");
      idp("ForgotPassword", { ClientId: CFG.clientId, Username: email })
        .then(function () {
          setStatus(T.codeSent, "ok");
          codeWrap.style.display = "block";
          sendBtn.textContent = "重新发送 Resend";
          sendBtn.disabled = false;
        })
        .catch(function (e) {
          setStatus(e.message || "发送失败", "err");
          sendBtn.disabled = false;
        });
    });

    resetBtn.addEventListener("click", function () {
      var code = codeInput.value.trim();
      if (!code || !np1.value) { setStatus("请输入验证码与新密码。", "err"); return; }
      resetBtn.disabled = true;
      setStatus("提交中… Submitting…", "");
      idp("ConfirmForgotPassword", {
        ClientId: CFG.clientId,
        Username: pendingEmail,
        ConfirmationCode: code,
        Password: np1.value,
      }).then(function () {
        setStatus("密码已重置，请用新密码登录。Password reset. Please sign in.", "ok");
        setTimeout(function () { showLogin(pendingEmail); }, 1200);
      }).catch(function (e) {
        setStatus(e.message || "重置失败", "err");
        resetBtn.disabled = false;
      });
    });

    back.addEventListener("click", function () { showLogin(emailInput.value.trim()); });
  }

  // ---- 退出登录 ----
  function signOut() {
    clearSession();
    idToken = "";
    allowedInstances = [];
    currentInstance = "";
    window.__APP_BOOTED__ = false;

    // 隐藏应用 UI（恢复防闪屏状态）
    var appHeader = document.querySelector("body > header");
    if (appHeader) appHeader.style.setProperty("display", "none", "important");
    var dashView = document.getElementById("viewDashboard");
    if (dashView) dashView.style.setProperty("display", "none", "important");

    // 移除实例切换器和退出按钮
    removeSwitcher();
    var signOutBtn = document.getElementById("btnSignOut");
    if (signOutBtn) signOutBtn.parentNode.removeChild(signOutBtn);

    // 重新显示登录遮罩
    if (!overlay || !overlay.parentNode) {
      overlay = h("div", { id: "authOverlay" }, [h("div", { id: "authCard" }, [])]);
      document.body.appendChild(overlay);
    } else {
      // 确保 authCard 存在
      if (!document.getElementById("authCard")) {
        overlay.appendChild(h("div", { id: "authCard" }, []));
      }
    }
    overlay.style.display = "";
    showLogin("");
  }

  // ---- 退出按钮（插入 header 最右侧）----
  function buildSignOutButton() {
    var existing = document.getElementById("btnSignOut");
    if (existing) return;
    var btn = h("button", { id: "btnSignOut" }, [tAuth("signOut")]);
    btn.addEventListener("click", signOut);
    var header = document.querySelector("header");
    if (header) {
      header.appendChild(btn);
    } else {
      document.body.appendChild(btn);
    }
  }

  // 语言切换时同步更新切换器 label 和退出按钮文字
  function refreshAuthLabels() {
    var switcher = document.getElementById("instSwitcher");
    if (switcher) {
      var label = switcher.querySelector(".inst-label");
      if (label) label.textContent = tAuth("switchInst") + ":";
    }
    var signOutBtn = document.getElementById("btnSignOut");
    if (signOutBtn) signOutBtn.textContent = tAuth("signOut");
    var refreshBtn = document.getElementById("btnRefreshList");
    if (refreshBtn) refreshBtn.textContent = "↻ " + tAuth("btnRefresh");
  }

  function bindLangChange() {
    var sel = document.getElementById("langSelect");
    if (!sel) return;
    sel.addEventListener("change", refreshAuthLabels);
  }

  // ---- 实例切换器（右上角）----
  function buildSwitcher(instances, activeInstance, onSwitch) {
    var existing = document.getElementById("instSwitcher");
    if (existing) existing.parentNode.removeChild(existing);

    var switcher = h("div", { id: "instSwitcher" }, []);
    switcher.appendChild(h("span", { class: "inst-label" }, [tAuth("switchInst") + ":"]));

    var sel = h("select", {}, []);
    instances.forEach(function (name) {
      var opt = h("option", { value: name }, [instanceLabels[name] || name]);
      if (name === activeInstance) opt.selected = true;
      sel.appendChild(opt);
    });
    sel.addEventListener("change", function () {
      onSwitch(sel.value);
    });
    switcher.appendChild(sel);

    // Channel 过滤（紧跟实例切换器右侧，与切换实例模式完全一致）
    switcher.appendChild(h("span", { class: "inst-label", style: "margin-left:12px" }, [tAuth("thChannel") + ":"]));
    var chSel = h("select", { id: "channelFilter" }, []);
    [["", tAuth("channelAll")], ["VOICE", "VOICE"], ["CHAT", "CHAT"]].forEach(function (pair) {
      chSel.appendChild(h("option", { value: pair[0] }, [pair[1]]));
    });
    chSel.addEventListener("change", function () {
      if (typeof window.__CONNECT_SWITCH_CHANNEL__ === "function") {
        window.__CONNECT_SWITCH_CHANNEL__(chSel.value);
      }
    });
    switcher.appendChild(chSel);

    // 刷新按钮：用当前渠道重新查询日志列表
    var refreshBtn = h("button", { id: "btnRefreshList",
      class: "nav-btn", style: "padding:5px 10px;font-size:12px;margin-left:4px" },
      ["↻ " + tAuth("btnRefresh")]);
    refreshBtn.addEventListener("click", function () {
      if (typeof window.__CONNECT_SWITCH_CHANNEL__ === "function") {
        window.__CONNECT_SWITCH_CHANNEL__(chSel.value);
      }
    });
    switcher.appendChild(refreshBtn);

    // 插入 header（语言切换器前），没有 header 则退回 body
    var header = document.querySelector("header");
    if (header) {
      var langPicker = header.querySelector(".lang-picker");
      if (langPicker) {
        header.insertBefore(switcher, langPicker);
      } else {
        header.appendChild(switcher);
      }
    } else {
      document.body.appendChild(switcher);
    }
    return switcher;
  }

  function removeSwitcher() {
    var el = document.getElementById("instSwitcher");
    if (el) el.parentNode.removeChild(el);
  }

  // ---- 认证成功：获取实例列表 -> 选实例 -> 加载数据 -> 启动 app ----
  function onAuthenticated(token) {
    idToken = token;
    setStatus(T.loading, "");

    var card = clearCard();
    card.appendChild(h("h2", {}, [T.title]));
    card.appendChild(h("div", { class: "sub" }, [T.loading]));
    statusEl = h("div", { id: "authStatus" }, []);
    card.appendChild(statusEl);

    // 登录后立刻用 RefreshToken 刷新 IdToken，拿到新的 1h token 再加载数据
    var rt = sessionStorage.getItem(SK_REFRESH);
    var proceed = function () {
      apiGet("/instances", {})
        .then(function (data) {
          allowedInstances = (data.instances || []).sort();
          instanceLabels = data.instanceLabels || {};
          if (!allowedInstances.length) {
            setStatus(T.noInstances, "err");
            return;
          }
          currentInstance = allowedInstances[0];
          loadAndBoot(currentInstance);
        })
        .catch(function (e) {
          setStatus("获取实例列表失败: " + (e.message || e), "err");
        });
    };
    if (rt) {
      refreshIdToken().then(proceed).catch(function () { proceed(); });
    } else {
      proceed();
    }
  }

  // ---- 加载指定实例数据并启动（或切换时重载）----
  function loadAndBoot(instanceName) {
    currentInstance = instanceName;

    // 更新右上角切换器（如果 app 已启动则切换器已在；否则等 bootApp 后再建）
    if (document.getElementById("instSwitcher")) {
      buildSwitcher(allowedInstances, currentInstance, onInstanceSwitch);
    }

    // 加载 Contact 摘要（只拉第一页）
    loadContactSummaries(instanceName)
      .then(function (page) {
        window.__CONNECT_AI_LOG_DATA__ = page.items;
        window.__CONNECT_INITIAL_NEXT_TOKEN__ = page.nextToken || null;
        window.__CONNECT_TOTAL_CONTACTS__ = page.totalContacts != null ? page.totalContacts : null;
        setStatus(T.loading + " (" + page.items.length + " contacts)", "");
        registerEventLoader(instanceName);
        registerDescribeContact(instanceName);
        registerFetchContactPage(instanceName);
        registerSearchContacts(instanceName);
        if (!window.__APP_BOOTED__) {
          bootApp(instanceName);
        } else {
          // app 已在，通知切换
          if (typeof window.__CONNECT_SWITCH_INSTANCE__ === "function") {
            window.__CONNECT_SWITCH_INSTANCE__(instanceName, page.items, page.nextToken, page.totalContacts);
          }
        }
      })
      .catch(function (e) {
        setStatus("加载联系人摘要失败: " + (e.message || e), "err");
      });
  }

  function onInstanceSwitch(newInstance) {
    if (newInstance === currentInstance) return;
    setStatus("切换到实例: " + newInstance + "…", "");
    // 切换实例期间显示 loading 覆盖层
    var loadingEl = document.getElementById("dashLoading");
    if (loadingEl) loadingEl.style.display = "flex";
    loadAndBoot(newInstance);
  }

  // ---- Contact 摘要分页：每次只拉一页，由 app.js 控制页码和 pageSize ----
  function parseContactItems(items) {
    var result = [];
    (items || []).forEach(function (item) {
      var sc = item.sourceCounts;
      if (typeof sc === "string") {
        try { sc = JSON.parse(sc); } catch (e) { sc = {}; }
      }
      var s = Object.assign({}, item);
      s.sourceCounts = sc || {};
      s._summaryOnly = true;
      result.push(s);
    });
    return result;
  }

  // 拉指定页（pageSize 条），nextToken=null 表示第一页
  // 返回 { items: [...], nextToken: "..." | null }
  function fetchContactPage(instanceName, pageSize, nextToken, channel) {
    var params = { instanceName: instanceName, pageSize: pageSize };
    if (nextToken) params.nextToken = nextToken;
    if (channel) params.channel = channel;
    return apiGet("/contacts", params).then(function (data) {
      var result = {
        items: parseContactItems(data.items || []),
        nextToken: data.nextToken || null,
      };
      if (data.totalContacts != null) result.totalContacts = data.totalContacts;
      return result;
    });
  }

  function loadContactSummaries(instanceName) {
    // 初始加载第一页（pageSize 由 app.js 控制，这里用默认值 15 启动）
    return fetchContactPage(instanceName, 15, null, null);
  }

  // 暴露给 app.js：按需拉指定页，支持 channel 过滤
  function registerFetchContactPage(instanceName) {
    window.__CONNECT_FETCH_CONTACT_PAGE__ = function (pageSize, nextToken, channel) {
      return fetchContactPage(instanceName, pageSize, nextToken, channel);
    };
  }

  // 暴露给 app.js：按 contactId 前缀搜索
  function registerSearchContacts(instanceName) {
    window.__CONNECT_SEARCH_CONTACTS__ = function (contactIdPrefix, pageSize) {
      var params = { instanceName: instanceName, contactId: contactIdPrefix, pageSize: pageSize || 20 };
      return apiGet("/contacts", params).then(function (data) {
        return { items: parseContactItems(data.items || []) };
      });
    };
  }

  // ---- 注册按需拉取事件明细的全局方法 ----
  // 流程: API Gateway 返回 S3 presigned URL -> 前端直接 fetch gzip JSON -> 解压解析
  function registerEventLoader(instanceName) {
    var _eventsCache = {};

    window.__CONNECT_LOAD_CONTACT_EVENTS__ = function (contactId, forceRefresh) {
      if (!forceRefresh && _eventsCache[contactId]) return _eventsCache[contactId];

      var p = apiGet("/contacts/" + encodeURIComponent(contactId) + "/events",
                     { instanceName: instanceName })
        .then(function (data) {
          var url = data.url;
          if (!url) throw new Error("events API 未返回 url");
          // 直接 fetch presigned URL，S3 会返回 gzip 压缩的 JSON，浏览器自动解压
          return fetch(url).then(function (resp) {
            if (!resp.ok) throw new Error("S3 fetch 失败: " + resp.status);
            return resp.json();
          });
        })
        .then(function (rows) {
          rows.sort(function (a, b) { return (a.timestamp || 0) - (b.timestamp || 0); });
          return rows;
        });
      p.catch(function () {
        if (_eventsCache[contactId] === p) delete _eventsCache[contactId];
      });
      _eventsCache[contactId] = p;
      return p;
    };
  }

  // ---- Amazon Connect DescribeContact（通过 API Gateway /describe 代理，Lambda 用执行角色调用）----
  function registerDescribeContact(instanceName) {
    var _descCache = {};

    window.__CONNECT_DESCRIBE_CONTACT__ = function (contactId, instanceId, forceRefresh) {
      if (!forceRefresh && _descCache[contactId]) return _descCache[contactId];

      var p = apiGet("/contacts/" + encodeURIComponent(contactId) + "/describe",
                     { instanceName: instanceName })
        .then(function (data) {
          // data = { Contact: {...} }
          return data;
        });
      p.catch(function () {
        if (_descCache[contactId] === p) delete _descCache[contactId];
      });
      _descCache[contactId] = p;
      return p;
    };
  }

  window.__CONNECT_FETCH_SUMMARY__ = function (contact) {
    // 通话摘要由后端 /describe 端点在调用 DescribeContact 时顺带读取 Contact Lens
    // S3 分析文件获得，直接复用已缓存的 describe 结果，取 contactSummary 字段。
    if (!contact) return Promise.resolve("");
    var cid = contact.Id || contact.id || "";
    if (!cid || typeof window.__CONNECT_DESCRIBE_CONTACT__ !== "function") {
      return Promise.resolve("");
    }
    return window.__CONNECT_DESCRIBE_CONTACT__(cid).then(function (data) {
      return (data && typeof data.contactSummary === "string")
        ? data.contactSummary.trim()
        : "";
    }).catch(function () { return ""; });
  };

  // ---- 数据就绪后动态加载 app.js ----
  function bootApp(instanceName) {
    window.__APP_BOOTED__ = true;

    // 翻译功能：CloudFront 版通过 API Gateway /translate 调用 Bedrock
    // 在 app.js 加载前注入配置，app.js 的 loadTranslateConfig() 会读取此值
    window.__CF_TRANSLATE_CFG__ = {
      available: !!(CFG.apiUrl),
      lang: (CFG.translateLang || "中文"),
      apiUrl: (CFG.apiUrl || ""),
    };

    var s = document.createElement("script");
    s.src = "./app.js";
    s.onload = function () {
      if (overlay && overlay.parentNode) overlay.parentNode.removeChild(overlay);
      // 移除防闪屏 <style> 标签（!important 无法通过 removeProperty 覆盖）
      var styles = document.querySelectorAll("style");
      for (var i = 0; i < styles.length; i++) {
        if (/#viewDashboard/.test(styles[i].textContent)) {
          styles[i].parentNode.removeChild(styles[i]);
          break;
        }
      }
      // 认证完成，显示应用 UI
      var appHeader = document.querySelector("body > header");
      if (appHeader) appHeader.style.removeProperty("display");
      var dashView = document.getElementById("viewDashboard");
      if (dashView) dashView.style.removeProperty("display");
      // 在 app 加载完成后建立右上角切换器（含 channel 过滤）和退出按钮
      buildSwitcher(allowedInstances, instanceName, onInstanceSwitch);
      buildSignOutButton();
      bindLangChange();   // 监听语言切换，同步更新切换器和退出按钮文字
    };
    s.onerror = function () { setStatus("加载 app.js 失败。", "err"); };
    document.body.appendChild(s);
  }

  // ---- 初始化：优先从 sessionStorage 恢复登录态 ----
  function init() {
    injectStyles();

    if (!CFG.region || !CFG.clientId || !CFG.userPoolId || !CFG.apiUrl) {
      // 配置缺失，直接显示错误
      overlay = h("div", { id: "authOverlay" }, [h("div", { id: "authCard" }, [])]);
      document.body.appendChild(overlay);
      clearCard().appendChild(h("h2", {}, [T.needConfig]));
      return;
    }

    // 尝试从 sessionStorage 恢复 token（刷新页面场景）
    var savedToken = loadSession();

    // IdToken 过期但有 RefreshToken -> 先静默刷新再恢复
    if (savedToken === "expired") {
      overlay = h("div", { id: "authOverlay" }, [h("div", { id: "authCard" }, [])]);
      document.body.appendChild(overlay);
      var card0 = document.getElementById("authCard");
      card0.appendChild(h("h2", {}, [T.title]));
      card0.appendChild(h("div", { class: "sub" }, [T.loading]));
      statusEl = h("div", { id: "authStatus" }, []);
      card0.appendChild(statusEl);
      refreshIdToken()
        .then(function () {
          // 刷新成功，移除遮罩后走正常恢复流程
          if (overlay && overlay.parentNode) overlay.parentNode.removeChild(overlay);
          init(); // 重新初始化，此时 loadSession() 返回有效 token
        })
        .catch(function () {
          // 刷新失败 -> 回到登录页
          clearSession();
          idToken = "";
          clearCard();
          showLogin("");
        });
      return;
    }

    if (savedToken && savedToken !== "expired") {
      // 有效 token：跳过登录框，直接用已有 token 恢复数据
      idToken = savedToken;
      // 创建遮罩用于显示加载状态（不显示登录表单）
      overlay = h("div", { id: "authOverlay" }, [h("div", { id: "authCard" }, [])]);
      document.body.appendChild(overlay);
      var card = document.getElementById("authCard");
      card.appendChild(h("h2", {}, [T.title]));
      card.appendChild(h("div", { class: "sub" }, [T.loading]));
      statusEl = h("div", { id: "authStatus" }, []);
      card.appendChild(statusEl);
      // 直接恢复实例列表和数据
      apiGet("/instances", {})
        .then(function (data) {
          allowedInstances = (data.instances || []).sort();
          instanceLabels = data.instanceLabels || {};
          if (!allowedInstances.length) {
            setStatus(T.noInstances, "err");
            return;
          }
          currentInstance = allowedInstances[0];
          loadAndBoot(currentInstance);
        })
        .catch(function (e) {
          // token 已失效，尝试用 RefreshToken 刷新
          var rt = sessionStorage.getItem(SK_REFRESH);
          if (rt) {
            refreshIdToken()
              .then(function () {
                if (overlay && overlay.parentNode) overlay.parentNode.removeChild(overlay);
                init();
              })
              .catch(function () {
                clearSession();
                idToken = "";
                clearCard();
                showLogin("");
              });
          } else {
            clearSession();
            idToken = "";
            clearCard();
            showLogin("");
          }
        });
      return;
    }

    // 没有有效 session，显示登录框
    overlay = h("div", { id: "authOverlay" }, [h("div", { id: "authCard" }, [])]);
    document.body.appendChild(overlay);
    showLogin("");
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
