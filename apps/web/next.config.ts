import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  distDir: ".next-dev",
  transpilePackages: ["@f1/shared-types", "@f1/ts-sdk", "@f1/ui"],
};

export default nextConfig;
