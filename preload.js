const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('mqttAPI', {
  connect: (config) => ipcRenderer.invoke('mqtt:connect', config),
  disconnect: () => ipcRenderer.invoke('mqtt:disconnect'),
  subscribe: (topic) => ipcRenderer.invoke('mqtt:subscribe', topic),
  unsubscribe: (topic) => ipcRenderer.invoke('mqtt:unsubscribe', topic),
  demoStart: () => ipcRenderer.invoke('demo:start'),
  demoStop: () => ipcRenderer.invoke('demo:stop'),
  onMessage: (cb) => ipcRenderer.on('mqtt:message', (_e, data) => cb(data)),
  onStatus: (cb) => ipcRenderer.on('mqtt:status', (_e, data) => cb(data)),
});

contextBridge.exposeInMainWorld('winAPI', {
  minimize: () => ipcRenderer.invoke('win:minimize'),
  toggleMaximize: () => ipcRenderer.invoke('win:maximize'),
  close: () => ipcRenderer.invoke('win:close'),
});
