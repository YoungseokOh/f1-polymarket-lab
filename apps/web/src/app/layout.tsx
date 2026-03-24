import type { Metadata } from "next";
import type { PropsWithChildren } from "react";

import { Header } from "./_components/header";
import { Sidebar } from "./_components/sidebar";
import "./globals.css";

export const metadata: Metadata = {
  title: "f1-polymarket-lab",
  description: "Formula 1 prediction-market research lab",
};

export default function RootLayout(props: PropsWithChildren) {
  return (
    <html lang="en">
      <body>
        <Sidebar />
        <div className="pt-14 lg:ml-[220px] lg:pt-0">
          <Header />
          <main className="min-h-[calc(100vh-4rem)]">{props.children}</main>
        </div>
      </body>
    </html>
  );
}
