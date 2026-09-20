import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";
import { App } from "../src/app/App";
import { AppProviders } from "../src/app/providers";

const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), {status, headers:{"Content-Type":"application/json"}});

beforeEach(()=>history.replaceState({},"","/admin/queue"));

it("shows subscription depth, schedules and safe callback outcomes", async () => {
  vi.spyOn(globalThis,"fetch").mockImplementation(async input => {
    const url=String(input);
    if(url.endsWith("/api/v1/admin/session")) return json({csrf_token:"csrf",expires_at:"2026-09-20T22:00:00Z"});
    if(url==="/api/v1/apps") return json([{id:"app_1",name:"Orders",delivery_mode:"queue",enabled:true,created_at:"2026-09-20T00:00:00Z",updated_at:"2026-09-20T00:00:00Z"}]);
    if(url.endsWith("/api/v1/admin/apps/app_1/subscriptions")) return json({items:[{id:"sub_1",app_id:"app_1",name:"orders",enabled:true,max_in_flight:10,max_batch_size:5,ordering_mode:"key",policy_version:1}]});
    if(url.endsWith("/metrics")) return json({available:3,in_flight:1,acknowledged:8,dead_letter:2});
    if(url.endsWith("/schedules")) return json({items:[{id:"qsch_1",name:"daily",enabled:true,cron_expression:"0 9 * * *",timezone:"Asia/Ho_Chi_Minh",event_type:"report.daily",next_run_at:"2026-09-21T02:00:00Z"}]});
    if(url.endsWith("/callbacks")) return json({items:[{id:"qcb_1",delivery_id:"qdl_1",event_id:"evt_1",generation:1,outcome:"success",status:"delivered",attempts:1,updated_at:"2026-09-20T00:00:00Z"}]});
    throw new Error(`unexpected ${url}`);
  });
  const user=userEvent.setup(); render(<AppProviders><App/></AppProviders>); await user.type(screen.getByLabelText("Bootstrap Admin token"),"bootstrap"); await user.click(screen.getByRole("button",{name:"Sign in"}));
  expect(await screen.findByRole("heading",{name:"Queue v2"})).toBeInTheDocument(); await screen.findByRole("option",{name:"Orders"}); await user.selectOptions(screen.getByLabelText("Application"),"app_1");
  expect(await screen.findByText("Priority ages one class/minute to a bounded maximum.")).toBeInTheDocument(); expect(await screen.findByText("daily")).toBeInTheDocument(); expect(await screen.findByText("qdl_1")).toBeInTheDocument();
});
