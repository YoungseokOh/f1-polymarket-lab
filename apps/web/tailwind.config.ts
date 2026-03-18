import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["'Space Grotesk'", "ui-sans-serif", "system-ui"],
      },
    },
  },
  plugins: [],
};

export default config;
