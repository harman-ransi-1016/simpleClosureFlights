// Slider tooltip transform — the RangeSlider value is a week index (0..370),
// this converts it to a human date string for the tooltip.
window.dccFunctions = window.dccFunctions || {};

window.dccFunctions.weekToDate = function(value) {
    // ALL_WEEKS starts at 2018-01-01 and increments by 7 days per index
    const start = new Date(Date.UTC(2018, 0, 1));
    const d = new Date(start);
    d.setUTCDate(d.getUTCDate() + value * 7);
    const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    return `${months[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.getUTCFullYear()}`;
};
