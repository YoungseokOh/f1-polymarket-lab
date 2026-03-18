import { sdk } from "@f1/ts-sdk";
import { Panel } from "@f1/ui";

export default async function SessionsPage() {
  const sessions = await sdk.sessions().catch(() => []);

  return (
    <main className="mx-auto max-w-6xl px-6 py-10">
      <Panel title="F1 Session Explorer" eyebrow="Silver">
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="text-slate-400">
              <tr>
                <th className="pb-3">Session</th>
                <th className="pb-3">Code</th>
                <th className="pb-3">Start</th>
                <th className="pb-3">Practice</th>
              </tr>
            </thead>
            <tbody>
              {sessions.map((session) => (
                <tr key={session.id} className="border-t border-white/10">
                  <td className="py-3">{session.sessionName}</td>
                  <td className="py-3">{session.sessionCode ?? "n/a"}</td>
                  <td className="py-3">{session.dateStartUtc ?? "n/a"}</td>
                  <td className="py-3">{session.isPractice ? "yes" : "no"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </main>
  );
}
