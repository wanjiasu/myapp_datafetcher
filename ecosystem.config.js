module.exports = {
  apps: [
    {
      name: 'tele_agent_fixture',
      script: 'env/bin/python',
      args: '-m uvicorn data_fetcher.api.app:app --host 0.0.0.0 --port 8003'
    }
  ]
};
