import Link from "next/link";
import { fetchUsersByTenant, type UsersResponse } from "@/lib/api";

export const revalidate = 0;

type PageProps = {
  params: Promise<{ tenantName: string }>;
  searchParams: Promise<{ q?: string; offset?: string; limit?: string }>;
};

export default async function TenantUsersPage({ params, searchParams }: PageProps) {
  const p = await params;
  const sp = await searchParams;
  const tenantName = decodeURIComponent(p.tenantName);
  const q = sp?.q || "";
  const limit = Number(sp?.limit || 50);
  const offset = Number(sp?.offset || 0);

  const data: UsersResponse = await fetchUsersByTenant(tenantName, q, limit, offset);

  return (
    <main style={{ padding: 24, maxWidth: 1080, margin: "0 auto" }}>
      <h1 style={{ marginBottom: 8, color: "#0f172a" }}>Users – {tenantName}</h1>

      <form
        action=""
        method="GET"
        style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 16 }}
      >
        <input
          type="text"
          name="q"
          defaultValue={q}
          placeholder="Search email/name…"
          style={{
            flex: "0 1 360px",
            padding: "8px 10px",
            border: "1px solid #ccc",
            borderRadius: 6,
          }}
        />
        <button
          type="submit"
          style={{
            padding: "8px 12px",
            border: "1px solid #1d4ed8",
            borderRadius: 6,
            background: "#2563eb",
            color: "#ffffff",
            cursor: "pointer",
          }}
        >
          Search
        </button>
        <Link href="/tenants" style={{ marginLeft: "auto" }}>
          ← Back to Tenants
        </Link>
      </form>

      <div style={{ overflowX: "auto" }}>
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            border: "1px solid #ddd",
          }}
        >
          <thead>
            <tr style={{ background: "#eff6ff" }}>
              <th style={th}>Email</th>
              <th style={th}>Name</th>
              <th style={th}>Tenant</th>
              <th style={th}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {(data.items || []).length === 0 ? (
              <tr>
                <td colSpan={4} style={td}>
                  No users found.
                </td>
              </tr>
            ) : (
              data.items.map((u, idx) => {
                const email = u.email || "";
                const href = email ? `/users/${encodeURIComponent(email)}` : "#";
                return (
                  <tr key={`${email}-${idx}`}>
                    <td style={tdMono}>{email}</td>
                    <td style={td}>{u.name || `${u.first_name || ""} ${u.last_name || ""}`.trim()}</td>
                    <td style={tdMono}>{u.tenantName || ""}</td>
                    <td style={td}>
                      {email ? <Link href={href}>Overview</Link> : <span style={{ color: "#888" }}>N/A</span>}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: 12, color: "#475569" }}>
        Showing {data.count} of {data.total}
      </div>
    </main>
  );
}

const th: React.CSSProperties = {
  textAlign: "left",
  padding: "10px 12px",
  borderBottom: "1px solid #eee",
  fontWeight: 600,
  fontSize: 14,
};

const td: React.CSSProperties = {
  padding: "10px 12px",
  borderBottom: "1px solid #f0f0f0",
  fontSize: 14,
};

const tdMono: React.CSSProperties = {
  ...td,
  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
};
