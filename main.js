const { app, BrowserWindow, ipcMain, Menu } = require('electron');
const path = require('path');
const mqtt = require('mqtt');

let mainWindow;
let mqttClient = null;
let demoTimer = null;
const DEMO_TOPIC = 'mqtt-charts/demo';

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 800,
    minHeight: 600,
    title: 'MQTT Charts',
    backgroundColor: '#0b0f17',
    frame: false,
    titleBarStyle: 'hidden',
    autoHideMenuBar: true,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false
    }
  });
  Menu.setApplicationMenu(null);
  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'));
}

/* ─── Window controls ─── */
ipcMain.handle('win:minimize', () => { if (mainWindow) mainWindow.minimize(); });
ipcMain.handle('win:maximize', () => { if (mainWindow) { if (mainWindow.isMaximized()) mainWindow.unmaximize(); else mainWindow.maximize(); } });
ipcMain.handle('win:close', () => { if (mainWindow) mainWindow.close(); });

app.whenReady().then(() => {
  createWindow();
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  stopDemo();
  if (mqttClient) mqttClient.end(true);
  if (process.platform !== 'darwin') app.quit();
});

/* ─── MQTT Connect ─── */
ipcMain.handle('mqtt:connect', async (_event, config) => {
  if (mqttClient) { mqttClient.end(true); mqttClient = null; }
  stopDemo();

  return new Promise((resolve) => {
    try {
      const opts = { reconnectPeriod: 5000, connectTimeout: 10000, clean: true };
      if (config.username) opts.username = config.username;
      if (config.password) opts.password = config.password;
      if (config.clientId) opts.clientId = config.clientId;

      mqttClient = mqtt.connect(config.url, opts);
      let resolved = false;

      mqttClient.on('connect', () => {
        if (mainWindow && !mainWindow.isDestroyed())
          mainWindow.webContents.send('mqtt:status', { status: 'connected' });
        if (!resolved) { resolved = true; resolve({ success: true }); }
      });

      mqttClient.on('error', (err) => {
        if (mainWindow && !mainWindow.isDestroyed())
          mainWindow.webContents.send('mqtt:status', { status: 'error', message: err.message });
        if (!resolved) { resolved = true; resolve({ success: false, error: err.message }); }
      });

      mqttClient.on('close', () => {
        if (mainWindow && !mainWindow.isDestroyed())
          mainWindow.webContents.send('mqtt:status', { status: 'disconnected' });
      });

      mqttClient.on('offline', () => {
        if (mainWindow && !mainWindow.isDestroyed())
          mainWindow.webContents.send('mqtt:status', { status: 'offline' });
      });

      mqttClient.on('message', (topic, payload) => {
        if (mainWindow && !mainWindow.isDestroyed()) {
          try {
            const json = JSON.parse(payload.toString());
            mainWindow.webContents.send('mqtt:message', { topic, payload: json });
          } catch (_e) {
            mainWindow.webContents.send('mqtt:message', { topic, payload: payload.toString(), raw: true });
          }
        }
      });
    } catch (err) {
      resolve({ success: false, error: err.message });
    }
  });
});

ipcMain.handle('mqtt:disconnect', async () => {
  stopDemo();
  if (mqttClient) { mqttClient.end(true); mqttClient = null; }
  return { success: true };
});

ipcMain.handle('mqtt:subscribe', async (_event, topic) => {
  if (mqttClient && mqttClient.connected) {
    return new Promise((resolve) => {
      mqttClient.subscribe(topic, { qos: 0 }, (err) => {
        resolve(err ? { success: false, error: err.message } : { success: true });
      });
    });
  }
  return { success: false, error: 'Not connected' };
});

ipcMain.handle('mqtt:unsubscribe', async (_event, topic) => {
  if (mqttClient && mqttClient.connected) {
    return new Promise((resolve) => {
      mqttClient.unsubscribe(topic, undefined, (err) => {
        resolve(err ? { success: false, error: err.message } : { success: true });
      });
    });
  }
  return { success: false, error: 'Not connected' };
});

/* ─── Demo Publisher (real MQTT round-trip) ─── */
ipcMain.handle('demo:start', async () => {
  if (demoTimer) return { success: true, topic: DEMO_TOPIC };
  if (!mqttClient || !mqttClient.connected) return { success: false, error: 'Not connected' };

  let t = 0;
  demoTimer = setInterval(() => {
    if (!mqttClient || !mqttClient.connected) { stopDemo(); return; }
    t += 0.15;
    const payload = {
      temperature: +(25 + Math.sin(t) * 5 + (Math.random() - 0.5) * 1.5).toFixed(2),
      humidity: +(60 + Math.cos(t * 0.7) * 10 + (Math.random() - 0.5) * 3).toFixed(2),
      pressure: +(1013 + Math.sin(t * 0.3) * 8 + (Math.random() - 0.5) * 2).toFixed(1),
      co2: Math.round(420 + Math.cos(t * 0.5) * 60 + (Math.random() - 0.5) * 15),
      pm25: +(35 + Math.sin(t * 1.2) * 20 + Math.abs(Math.random() - 0.5) * 10).toFixed(1),
    };
    mqttClient.publish(DEMO_TOPIC, JSON.stringify(payload), { qos: 0 });
  }, 250);

  return { success: true, topic: DEMO_TOPIC };
});

ipcMain.handle('demo:stop', async () => {
  stopDemo();
  return { success: true };
});

function stopDemo() {
  if (demoTimer) { clearInterval(demoTimer); demoTimer = null; }
}
