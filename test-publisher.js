/* ═══════════════════════════════════════════
   Test Publisher — 每秒向 broker 发送多字段 JSON
   用法: node test-publisher.js
   ═══════════════════════════════════════════ */
const mqtt = require('mqtt');

const BROKER = process.argv[2] || 'mqtt://broker.hovemq.com:1883'.replace('hovemq', 'hivemq');
const TOPIC = process.argv[3] || 'sensor/data';

console.log(`Connecting to ${BROKER} ...`);
const client = mqtt.connect(BROKER, { clientId: 'test-pub-' + Math.random().toString(16).slice(2, 8) });

client.on('connect', () => {
  console.log(`Connected! Publishing to topic: "${TOPIC}" every 1s`);
  console.log('In the app, add a chart with this topic in Auto Multi-Line mode.');
  console.log('Press Ctrl+C to stop.\n');

  let t = 0;
  setInterval(() => {
    t += 0.1;
    // 模拟多字段传感器数据 —— 自动模式下会生成 5 条折线
    const payload = {
      temperature: 25 + Math.sin(t) * 5 + (Math.random() - 0.5) * 1.5,
      humidity: 60 + Math.cos(t * 0.7) * 10 + (Math.random() - 0.5) * 3,
      pressure: 1013 + Math.sin(t * 0.3) * 8 + (Math.random() - 0.5) * 2,
      co2: 420 + Math.cos(t * 0.5) * 60 + (Math.random() - 0.5) * 15,
      pm25: 35 + Math.sin(t * 1.2) * 20 + Math.abs(Math.random() - 0.5) * 10,
    };
    client.publish(TOPIC, JSON.stringify(payload), { qos: 0 });
    console.log(`[${new Date().toLocaleTimeString()}] ${JSON.stringify(payload)}`);
  }, 1000);
});

client.on('error', (err) => {
  console.error('Connection error:', err.message);
  console.error('Try another broker:');
  console.error('  node test-publisher.js mqtt://broker.emqx.io:1883');
});

client.on('close', () => console.log('Disconnected'));
