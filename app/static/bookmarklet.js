// Unwrapped bookmarklet code for evaluation
function downloadData() {
    let title = document.querySelector('.race-head-title.ui-mainview-block')?.innerText.trim() || "race_data";
    let meta = document.querySelector('.race-meta.ui-mainview-block')?.innerText.trim() || "No meta found";
    let runners = [...document.querySelectorAll('.runners-list')].map(el => el.outerHTML).join("\n") || "No runners found";
    let track = document.querySelector('.ui-left')?.innerText.trim() || "Track not found";
    let content = {
        title,
        meta,
        runners_html: runners,
        track
    };
    window.__BOOKMARKLET_DATA__ = content;
}
setTimeout(downloadData, 2000);