/* global process, setInterval */

process.send?.({ type: 'worker.ready', workerId: process.argv[2], pid: process.pid });
setInterval(() => {}, 1_000);
