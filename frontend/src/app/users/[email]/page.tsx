import Link from "next/link";
import { fetchUserOverview, type UserOverviewResponse, formatDate } from "@/lib/api";

export const revalidate = 0;

type PageProps = {
  params: Promise<{ email: string }>;
};

export default async function UserOverviewPage({ params }: PageProps) {
  const p = await params;
  const email = decodeURIComponent(p.email);
  const data: UserOverviewResponse = await fetchUserOverview(email);

  const { user, runs, latest_run, latest_run_events, latest_run_decisions, latest_run_email_attempts, latest_run_features } =
    data || ({} as UserOverviewResponse);

  return (
    <main style={{ padding: 24, maxWidth: 1200, margin: "0 auto" }}>
      <header style={{ display: "flex", alignItems: "baseline", gap: 12 }}>
        <h1 style={{ marginBottom: 4 }}>User Overview</h1>
        <code style={{ color: "#1e3a8a" }}>{user?.email}</code>
      </header>

      <section style={{ marginTop: 16 }}>
        <h2 style={{ marginBottom: 8, fontSize: 18 }}>Recent Runs</h2>
        <div style={{ overflowX: "auto" }}>
          <table style={table}>
            <thead>
              <tr style={theadTr}>
                <th style={th}>Run ID</th>
                <th style={th}>Status</th>
                <th style={th}>Started</th>
                <th style={th}>Finished</th>
              </tr>
            </thead>
            <tbody>
              {(runs || []).length === 0 ? (
                <tr>
                  <td colSpan={4} style={td}>
                    No runs found.
                  </td>
                </tr>
              ) : (
                (runs || []).map((r) => (
                  <tr key={r.id}>
                    <td style={tdMono}>{r.id}</td>
                    <td style={td}>{r.status || ""}</td>
                    <td style={td}>{formatDate(r.started_at)}</td>
                    <td style={td}>{formatDate(r.finished_at)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      {latest_run ? (
        <>
          <section style={{ marginTop: 24 }}>
            <h2 style={{ marginBottom: 8, fontSize: 18 }}>Latest Run Details</h2>
            <div style={{ color: "#555", marginBottom: 8 }}>
              <div>
                <b>Run ID:</b> <code>{latest_run.id}</code>
              </div>
              <div>
                <b>Status:</b> {latest_run.status} | <b>Started:</b> {formatDate(latest_run.started_at)} | <b>Finished:</b>{" "}
                {formatDate(latest_run.finished_at)}
              </div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>
              <div>
                <h3 style={h3}>Events</h3>
                <div style={{ overflowX: "auto" }}>
                  <table style={table}>
                    <thead>
                      <tr style={theadTr}>
                        <th style={th}>Time</th>
                        <th style={th}>Type</th>
                        <th style={th}>Payload</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(latest_run_events || []).length === 0 ? (
                        <tr>
                          <td colSpan={3} style={td}>
                            No events.
                          </td>
                        </tr>
                      ) : (
                        (latest_run_events || []).map((e) => (
                          <tr key={e.id}>
                            <td style={td}>{formatDate(e.occurred_at)}</td>
                            <td style={tdMono}>{e.event_type}</td>
                            <td style={tdSmallJson}>{truncate(JSON.stringify(e.payload))}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              <div>
                <h3 style={h3}>Decisions</h3>
                <div style={{ overflowX: "auto" }}>
                  <table style={table}>
                    <thead>
                      <tr style={theadTr}>
                        <th style={th}>Time</th>
                        <th style={th}>Rule</th>
                        <th style={th}>Decision</th>
                        <th style={th}>Rationale</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(latest_run_decisions || []).length === 0 ? (
                        <tr>
                          <td colSpan={4} style={td}>
                            No decisions.
                          </td>
                        </tr>
                      ) : (
                        (latest_run_decisions || []).map((d, idx) => (
                          // no id in shape, use index
                          <tr key={idx}>
                            <td style={td}>{formatDate(d.decided_at)}</td>
                            <td style={tdMono}>{d.rule}</td>
                            <td style={tdMono}>{d.decision}</td>
                            <td style={tdSmallJson}>{truncate(JSON.stringify(d.rationale))}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              <div>
                <h3 style={h3}>Email Attempts (latest run)</h3>
                <div style={{ overflowX: "auto" }}>
                  <table style={table}>
                    <thead>
                      <tr style={theadTr}>
                        <th style={th}>Template</th>
                        <th style={th}>Stage</th>
                        <th style={th}>Status</th>
                        <th style={th}>Reason</th>
                        <th style={th}>Created</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(latest_run_email_attempts || []).length === 0 ? (
                        <tr>
                          <td colSpan={5} style={td}>
                            No attempts.
                          </td>
                        </tr>
                      ) : (
                        (latest_run_email_attempts || []).map((a, idx) => (
                          <tr key={idx}>
                            <td style={tdMono}>{a.template_key}</td>
                            <td style={tdMono}>{a.stage}</td>
                            <td style={tdMono}>{a.status}</td>
                            <td style={td}>{a.reason || ""}</td>
                            <td style={td}>{formatDate(a.created_at)}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              <div>
                <h3 style={h3}>Features (latest run)</h3>
                <div style={{ overflowX: "auto" }}>
                  <table style={table}>
                    <thead>
                      <tr style={theadTr}>
                        <th style={th}>Name</th>
                        <th style={th}>Value</th>
                        <th style={th}>Computed At</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(latest_run_features || []).length === 0 ? (
                        <tr>
                          <td colSpan={3} style={td}>
                            No features.
                          </td>
                        </tr>
                      ) : (
                        (latest_run_features || []).map((f, idx) => (
                          <tr key={idx}>
                            <td style={tdMono}>{f.name}</td>
                            <td style={tdSmallJson}>{truncate(JSON.stringify(f.value))}</td>
                            <td style={td}>{formatDate(f.computed_at)}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </section>
        </>
      ) : null}

      <div style={{ marginTop: 16 }}>
        <Link href={`/tenants`}>← Back to Tenants</Link>
      </div>
    </main>
  );
}

function truncate(s: string, max = 200) {
  if (s.length <= max) return s;
  return s.slice(0, max) + "…";
}

const table: React.CSSProperties = {
  width: "100%",
  borderCollapse: "collapse",
  border: "1px solid #ddd",
};

const theadTr: React.CSSProperties = {
  background: "#fafafa",
};

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
  verticalAlign: "top",
};

const tdMono: React.CSSProperties = {
  ...td,
  fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
};

const tdSmallJson: React.CSSProperties = {
  ...td,
  fontSize: 12,
  color: "#0f172a",
  whiteSpace: "pre-wrap",
  wordBreak: "break-word",
};

const h3: React.CSSProperties = {
  margin: "16px 0 8px",
  fontSize: 16,
  fontWeight: 600,
};
