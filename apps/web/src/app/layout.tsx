import type { Metadata } from "next";
import type { PropsWithChildren } from "react";

import "./globals.css";

export const metadata: Metadata = {
  title: "f1-polymarket-lab",
  description: "Formula 1 prediction-market research lab",
};

export default function RootLayout(props: PropsWithChildren) {
  return (
    <html lang="en">
      <body>{props.children}</body>
    </html>
  );
}
