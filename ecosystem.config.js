module.exports = {
  apps: [
    {
      name: 'tele_agent_fixture',
      script: 'python',
      args: '-m uvicorn data_fetcher.api.app:app --host 0.0.0.0 --port 8003',
      cwd: '.',
      exec_mode: 'fork',
      interpreter: 'none',
      env: {
        HOST: '0.0.0.0',
        PORT: '8003'
      },
      autorestart: true,
      watch: false,
      max_memory_restart: '300M'
    }
  ]
};
