/** @type {import('next').NextConfig} */
const nextConfig = {
  "rewrites": [
    {
      "source": "/api/:path*",
      "destination": "https://fintech-poc.onrender.com/api/:path*"
    }
  ]
}

export default nextConfig