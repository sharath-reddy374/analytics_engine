import Link from "next/link";

export const revalidate = 0;

export default function Home() {
  return (
    <main style={{ padding: 24, maxWidth: 960, margin: "0 auto" }}>
      <h1 style={{ marginBottom: 8 }}>EdYou Ops Dashboard</h1>
      <p style={{ color: "#555", marginBottom: 16 }}>
        Multi-tenant ops UI. Read-only. No outbound emails/SMS/notifications are sent.
      </p>

      <ul style={{ listStyle: "none", padding: 0, display: "grid", gap: 12 }}>
        <li>
          <Link
            href="/tenants"
            style={{
              display: "inline-block",
              padding: "10px 12px",
              border: "1px solid #ccc",
              borderRadius: 8,
              background: "#f7f7f7",
            }}
          >
            View Tenants →
          </Link>
        </li>
        <li style={{ color: "#666", fontSize: 14 }}>
          Backend API base: {process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:8001"}
        </li>
      </ul>
    </main>
  );
}
