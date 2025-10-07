import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: "class",
  content: [
    "./src/app/**/*.{ts,tsx}",
    "./src/components/**/*.{ts,tsx}",
    "./src/lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // White & Blue palette
        border: "#e5e7eb", // slate-200
        input: "#e5e7eb",
        ring: "#2563eb", // blue-600
        background: "#ffffff",
        foreground: "#0f172a", // slate-900

        primary: {
          DEFAULT: "#2563eb", // blue-600
          foreground: "#ffffff",
        },
        secondary: {
          DEFAULT: "#eff6ff", // blue-50
          foreground: "#1e40af", // blue-800
        },
        muted: {
          DEFAULT: "#f1f5f9", // slate-100
          foreground: "#475569", // slate-600 (darker than previous grey)
        },
        accent: {
          DEFAULT: "#dbeafe", // blue-100
          foreground: "#1e3a8a", // blue-900
        },
        destructive: {
          DEFAULT: "#ef4444", // red-500
          foreground: "#ffffff",
        },
        card: {
          DEFAULT: "#ffffff",
          foreground: "#0f172a",
        },
      },
      borderRadius: {
        lg: "0.5rem",
        md: "0.375rem",
        sm: "0.25rem",
      },
    },
  },
  plugins: [],
};

export default config;
