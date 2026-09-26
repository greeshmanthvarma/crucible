import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App";
import { bootstrapFromLocation } from "./auth/session";
import "./index.css";

void bootstrapFromLocation().finally(() => {
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <App />
    </StrictMode>,
  );
});
