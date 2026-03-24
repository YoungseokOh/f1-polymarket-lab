import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/**/*.{ts,tsx}",
    "../../packages/ui/src/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ["'Space Grotesk'", "ui-sans-serif", "system-ui"],
      },
      colors: {
        f1: {
          red: "#e10600",
          "red-dark": "#b30500",
          dark: "#15151e",
          surface: "#1e1e2e",
          "surface-elevated": "#262637",
          border: "rgba(255,255,255,0.08)",
        },
        race: {
          green: "#00d747",
          yellow: "#ffd600",
          red: "#e10600",
        },
      },
    },
  },
  plugins: [],
};

export default config;
