import Link from "next/link";
import { fetchTenants, type TenantItem } from "@/lib/api";
import { fetchMetrics } from "@/lib/metrics";
import { KpiCard } from "@/components/kpi-card";
import { LineSeriesChart } from "@/components/line-chart";
import { BarSimpleChart } from "@/components/bar-chart";

export const revalidate = 0;

type PageProps = {
  searchParams: Promise<{ days?: string; tenantName?: string }>;
};

export default async function DashboardPage({ searchParams }: PageProps) {
  const sp = await searchParams;
  const days = Number(sp?.days ?? 7);
  const tenantName = sp?.tenantName ?? "";
  const [tenantsResp, metrics] = await Promise.all([
    fetchTenants(),
    fetchMetrics({ days, tenantName: tenantName || undefined }),
  ]);

  const decisionsByRule = (metrics.distributions.decisions_by_rule || []).map((d) => ({
    name: d.rule,
    value: Number(d.c || 0),
  }));

  const attemptsByTplStatus = (metrics.distributions.attempts_by_template_status || []).map((a) => ({
    name: `${a.template_key}:${a.status}`,
    value: Number(a.c || 0),
  }));

  return (
    <main className="p-6 max-w-7xl mx-auto">
      <header className="mb-4 flex items-center gap-2">
        <h1 className="text-xl font-semibold">Analytics Overview</h1>
        <span className="text-sm text-slate-500">(read-only)</span>
        <div className="ml-auto">
          <Link href="/" className="text-sm text-blue-600 hover:underline">
            ← Home
          </Link>
        </div>
      </header>

      {/* Filters */}
      <form className="mb-5 flex flex-wrap gap-3 items-end" action="/dashboard" method="GET">
        <div className="flex flex-col">
          <label className="text-sm text-slate-600 mb-1" htmlFor="tenantName">
            Tenant
          </label>
          <select
            id="tenantName"
            name="tenantName"
            defaultValue={tenantName}
            className="border border-border rounded-md bg-card text-foreground px-2 py-1"
          >
            <option value="">All tenants</option>
            {(tenantsResp.tenants || []).map((t: TenantItem) => (
              <option key={t.tenantName} value={t.tenantName}>
                {t.tenantName} ({t.count})
              </option>
            ))}
          </select>
        </div>

        <div className="flex flex-col">
          <label className="text-sm text-slate-600 mb-1" htmlFor="days">
            Date range
          </label>
          <select
            id="days"
            name="days"
            defaultValue={String(days)}
            className="border border-border rounded-md bg-card text-foreground px-2 py-1"
          >
            <option value="7">Last 7 days</option>
            <option value="14">Last 14 days</option>
            <option value="30">Last 30 days</option>
          </select>
        </div>

        <button
          type="submit"
          className="rounded-md bg-blue-600 text-white px-3 py-1 hover:bg-blue-700 transition-colors"
        >
          Apply
        </button>
      </form>

      {/* KPIs */}
      <section className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 mb-5">
        <KpiCard label="Total Users" value={metrics.kpis.total_users} />
        <KpiCard label="Active Users (7d)" value={metrics.kpis.active_users_7d} />
        <KpiCard label="Runs (24h)" value={metrics.kpis.runs_24h} hint={`Last ${days}d: ${metrics.kpis.runs_7d}`} />
        <KpiCard label="Events (24h)" value={metrics.kpis.events_24h} hint={`Last ${days}d: ${metrics.kpis.events_7d}`} />
        <KpiCard
          label="Decisions (24h)"
          value={metrics.kpis.decisions_24h}
          hint={`Last ${days}d: ${metrics.kpis.decisions_7d}`}
        />
        <KpiCard
          label="Queued Attempts (24h)"
          value={metrics.kpis.queued_attempts_24h}
          hint={`Last ${days}d: ${metrics.kpis.queued_attempts_7d}`}
        />
      </section>

      {/* Time-series */}
      <section className="grid grid-cols-1 lg:grid-cols-3 gap-3 mb-5">
        <LineSeriesChart title="Runs per day" data={metrics.series.runs_by_day} />
        <LineSeriesChart title="Events per day" data={metrics.series.events_by_day} color="#f59e0b" />
        <LineSeriesChart title="Decisions per day" data={metrics.series.decisions_by_day} color="#10b981" />
      </section>

      {/* Distributions */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-3 mb-6">
        <BarSimpleChart title="Decisions by Rule" data={decisionsByRule} />
        <BarSimpleChart title="Attempts by Template and Status" data={attemptsByTplStatus} color="#6366f1" />
      </section>

      <footer className="text-sm text-muted-foreground">
        Filters applied: days={metrics.filters.days}
        {metrics.filters.tenantName ? `, tenant=${metrics.filters.tenantName}` : ""}
      </footer>
    </main>
  );
}
