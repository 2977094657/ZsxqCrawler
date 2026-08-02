const path = require("path");

const projectRoot = __dirname;
const frontendRoot = path.join(projectRoot, "frontend");
const pythonExecutable = path.join(
  projectRoot,
  ".venv",
  process.platform === "win32" ? "Scripts/python.exe" : "bin/python",
);

module.exports = {
  apps: [
    {
      name: "zsxq-backend",
      cwd: projectRoot,
      script: pythonExecutable,
      args: "main.py",
      interpreter: "none",
      env: {
        PYTHONUTF8: "1",
        PYTHONIOENCODING: "utf-8",
      },
      autorestart: true,
      restart_delay: 3000,
      max_restarts: 10,
      time: true,
    },
    {
      name: "zsxq-frontend",
      cwd: frontendRoot,
      script: path.join(frontendRoot, "node_modules/next/dist/bin/next"),
      args: "start -p 3060",
      interpreter: process.execPath,
      env: {
        NODE_ENV: "production",
      },
      autorestart: true,
      restart_delay: 3000,
      max_restarts: 10,
      time: true,
    },
  ],
};
