import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  transpilePackages: ["@f1/shared-types", "@f1/ts-sdk", "@f1/ui"],
};

export default nextConfig;
