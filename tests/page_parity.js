// Runs the page's detect() on the embedded data and prints flagged groups as JSON.
const fs = require("fs");
const html = fs.readFileSync(process.argv[2], "utf8");
const DATA = JSON.parse(html.match(/const DATA=(.*?);\nconst BASE/s)[1]);
const BASE = "27A1", CAND = "27A2";
const code = html.match(/function erfc[\s\S]*?\nfunction draw/)[0].replace(/\nfunction draw$/, "");
const detect = new Function("DATA", "BASE", "CAND", code + "; return detect;")(DATA, BASE, CAND);
const out = {};
for (const [eff, alp] of [[0.05, 0.01], [0.0, 0.01], [0.0, 0.5], [0.15, 0.01], [0.3, 0.01]]) {
  out[`${eff}|${alp}`] = detect(eff, alp).filter(r => r.flag).map(r => r.k).sort();
}
console.log(JSON.stringify(out));
