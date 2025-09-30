import Link from "next/link";
import { fetchTenants, type TenantItem } from "@/lib/api";

export const revalidate = 0; // always fresh

export default async function TenantsPage() {
  const data = await fetchTenants();
  const tenants: TenantItem[] = data.tenants || [];

  return (
    <main style={{ padding: 24, maxWidth: 960, margin: "0 auto" }}>
      <h1 style={{ marginBottom: 8 }}>Tenants</h1>
      <p style={{ color: "#555", marginBottom: 16 }}>
        Total users: <b>{data.total_users}</b>
      </p>

      <div style={{ overflowX: "auto" }}>
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            border: "1px solid #ddd",
          }}
        >
          <thead>
            <tr style={{ background: "#fafafa" }}>
              <th style={th}>Tenant</th>
              <th style={th}>User Count</th>
              <th style={th}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {tenants.length === 0 ? (
              <tr>
                <td colSpan={3} style={td}>
                  No tenants found.
                </td>
              </tr>
            ) : (
              tenants.map((t) => {
                const href = `/tenants/${encodeURIComponent(
                  t.tenantName
                )}/users`;
                return (
                  <tr key={t.tenantName}>
                    <td style={tdMono}>{t.tenantName}</td>
                    <td style={tdCenter}>{t.count}</td>
                    <td style={td}>
                      <Link href={href}>View Users</Link>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: 16 }}>
        <Link href="/">← Back to Home</Link>
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

const tdCenter: React.CSSProperties = {
  ...td,
  textAlign: "center",
};
