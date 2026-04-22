const RAILWAY_URL = 'https://YOUR-APP.up.railway.app/run-by-insert-time';
const SECRET = 'YOUR_SECRET';

function notifyRailwayHourly() {
  const url = `${RAILWAY_URL}?secret=${encodeURIComponent(SECRET)}`;
  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    muteHttpExceptions: true,
  });

  Logger.log(res.getResponseCode());
  Logger.log(res.getContentText());
}
