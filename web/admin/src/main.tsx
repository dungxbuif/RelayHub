import React from "react";
import ReactDOM from "react-dom/client";

import { App } from "./app/App";
import { AppProviders } from "./app/providers";
import "./styles/tokens.css";
import "./styles/global.css";

const root = document.getElementById("root");

if (!root) {
  throw new Error("RelayHub Admin root element is missing");
}

ReactDOM.createRoot(root).render(
  <React.StrictMode>
    <AppProviders>
      <App />
    </AppProviders>
  </React.StrictMode>,
);
