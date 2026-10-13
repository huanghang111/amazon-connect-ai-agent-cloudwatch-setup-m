/* 站点运行时配置(默认值)
 *
 * 构建脚本 setup-connect-ai-agent-logs-analysis.sh 会依据 config.env 的
 * UI_DEFAULT_LANG，在输出目录(dist)里重新生成此文件以覆盖默认值。
 *
 * defaultLang: 页面首次打开时的界面语言。
 *   支持 中文 / English / Español / Italiano / Deutsch,
 *   也接受语言代码 zh / en / es / it / de。
 *
 * csatAttribute: 「CSAT 满意度评分」列对应的 Amazon Connect 联系人属性键名。
 *   不写死，可按账号/Contact Flow 实际写入的键名调整;来自 config.env 的
 *   CSAT_ATTRIBUTE_KEY。留空/缺省时前端回退为 "botevaluation"。
 */
window.__UI_CONFIG__ = { defaultLang: "中文", csatAttribute: "botevaluation" };
