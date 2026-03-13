"use client";

import { forceCenter, forceCollide, forceLink, forceManyBody, forceSimulation } from "d3-force";
import { useEffect, useMemo, useRef, useState } from "react";

type Conflict = {
  conflict_id: string;
  minister_name: string;
  company_cnpj_basico: string;
  company_name: string;
  linked_entity_name: string;
  linked_entity_type: string;
  link_chain: string | null;
  link_degree: number;
  risk_score: number | null;
  red_flag: boolean;
};

type GraphNode = {
  id: string;
  label: string;
  kind: "minister" | "company" | "party" | "counsel" | "representative";
  x?: number;
  y?: number;
};

type GraphLink = {
  id: string;
  source: string | GraphNode;
  target: string | GraphNode;
  degree: number;
  riskScore: number | null;
  redFlag: boolean;
};

function nodeColor(kind: GraphNode["kind"]): string {
  if (kind === "minister") return "#0f172a";
  if (kind === "company") return "#002776";
  if (kind === "party") return "#946300";
  if (kind === "counsel") return "#007D30";
  return "#7c2d12";
}

function nodeRadius(kind: GraphNode["kind"]): number {
  if (kind === "minister") return 13;
  if (kind === "company") return 10;
  if (kind === "representative") return 8;
  return 9;
}

function edgeColor(link: GraphLink): string {
  if (link.redFlag) return "#dc2626";
  if (link.degree >= 3) return "#d97706";
  if (link.degree === 2) return "#002D9E";
  return "#94a3b8";
}

function buildGraph(conflicts: Conflict[]) {
  const nodeMap = new Map<string, GraphNode>();
  const linkMap = new Map<string, GraphLink>();

  const ensureNode = (node: GraphNode) => {
    if (!nodeMap.has(node.id)) {
      nodeMap.set(node.id, node);
    }
  };

  for (const conflict of conflicts) {
    const chainParts = (conflict.link_chain ?? "")
      .split(" -> ")
      .map((part) => part.trim())
      .filter(Boolean);
    if (chainParts.length < 2) {
      continue;
    }
    for (let index = 0; index < chainParts.length; index += 1) {
      const label = chainParts[index];
      let kind: GraphNode["kind"] = "company";
      let id = `company:${label}`;
      if (index === 0) {
        kind = "minister";
        id = `minister:${label}`;
      } else if (index === chainParts.length - 1) {
        kind = conflict.linked_entity_type === "party" ? "party" : "counsel";
        id = `${kind}:${conflict.linked_entity_name}`;
      } else if (label.startsWith("(repr.) ")) {
        kind = "representative";
        id = `representative:${label}`;
      } else if (label === (conflict.company_name || conflict.company_cnpj_basico)) {
        id = `company:${conflict.company_cnpj_basico}`;
      }
      ensureNode({ id, label, kind });
      if (index === 0) {
        continue;
      }
      const sourceLabel = chainParts[index - 1];
      const sourceId =
        index - 1 === 0
          ? `minister:${sourceLabel}`
          : sourceLabel.startsWith("(repr.) ")
            ? `representative:${sourceLabel}`
            : sourceLabel === (conflict.company_name || conflict.company_cnpj_basico)
              ? `company:${conflict.company_cnpj_basico}`
              : `company:${sourceLabel}`;
      const linkId = `${sourceId}->${id}`;
      if (!linkMap.has(linkId)) {
        linkMap.set(linkId, {
          id: linkId,
          source: sourceId,
          target: id,
          degree: conflict.link_degree,
          riskScore: conflict.risk_score,
          redFlag: conflict.red_flag,
        });
      }
    }
  }

  return { nodes: Array.from(nodeMap.values()), links: Array.from(linkMap.values()) };
}

function resolveNode(nodeRef: string | GraphNode): GraphNode {
  return typeof nodeRef === "string" ? { id: nodeRef, label: nodeRef, kind: "company" } : nodeRef;
}

export function CorporateNetworkGraph({ conflicts }: { conflicts: Conflict[] }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;
    const updateSize = () => {
      const next = {
        width: Math.floor(element.clientWidth),
        height: Math.max(360, Math.floor(element.clientHeight || 420)),
      };
      setSize((current) => (current.width === next.width && current.height === next.height ? current : next));
    };
    updateSize();
    const observer = new ResizeObserver(updateSize);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const graph = useMemo(() => {
    if (size.width <= 0 || conflicts.length === 0) {
      return { nodes: [], links: [] };
    }
    const next = buildGraph(conflicts);
    const nodes = next.nodes.map((node) => ({ ...node }));
    const links = next.links.map((link) => ({ ...link }));
    const simulation = forceSimulation(nodes)
      .force("link", forceLink<GraphNode, GraphLink>(links).id((node) => node.id).distance((link) => 70 + link.degree * 18))
      .force("charge", forceManyBody<GraphNode>().strength(-320))
      .force("center", forceCenter(size.width / 2, size.height / 2))
      .force("collision", forceCollide<GraphNode>().radius((node) => nodeRadius(node.kind) + 12));
    for (let step = 0; step < 220; step += 1) {
      simulation.tick();
    }
    simulation.stop();
    return { nodes, links };
  }, [conflicts, size.height, size.width]);

  return (
    <section className="rounded-3xl border border-slate-200 bg-white p-5 shadow-sm">
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-slate-900">Grafo dos vinculos da pagina</h3>
          <p className="text-sm text-slate-600">
            Layout por forca com caminhos reais de <span className="font-medium">link_chain</span>. Graus 3+ usam score com decay.
          </p>
        </div>
        <div className="flex flex-wrap gap-2 text-xs text-slate-600">
          <span className="rounded-full bg-slate-100 px-3 py-1">Nos: {graph.nodes.length}</span>
          <span className="rounded-full bg-slate-100 px-3 py-1">Arestas: {graph.links.length}</span>
        </div>
      </div>
      <div ref={containerRef} className="h-[420px] w-full overflow-hidden rounded-2xl border border-slate-200 bg-slate-50">
        {graph.nodes.length === 0 ? (
          <div className="flex h-full items-center justify-center px-6 text-sm text-slate-500">
            Nenhum caminho auditavel disponivel para montar o grafo com os filtros atuais.
          </div>
        ) : (
          <svg width={size.width} height={size.height} viewBox={`0 0 ${size.width} ${size.height}`} className="h-full w-full" role="img" aria-label={`Mapa de conexoes empresariais com ${graph.nodes.length} entidades`}>
            {graph.links.map((link) => {
              const source = resolveNode(link.source);
              const target = resolveNode(link.target);
              return (
                <line
                  key={link.id}
                  x1={source.x ?? 0}
                  y1={source.y ?? 0}
                  x2={target.x ?? 0}
                  y2={target.y ?? 0}
                  stroke={edgeColor(link)}
                  strokeOpacity={0.5}
                  strokeWidth={link.redFlag ? 2.8 : 1.8}
                >
                  <title>
                    {`grau ${link.degree}${link.riskScore != null ? ` | score ${link.riskScore.toFixed(3)}` : ""}`}
                  </title>
                </line>
              );
            })}
            {graph.nodes.map((node) => (
              <g key={node.id} transform={`translate(${node.x ?? 0}, ${node.y ?? 0})`}>
                <circle r={nodeRadius(node.kind)} fill={nodeColor(node.kind)} fillOpacity={0.92} />
                <text
                  x={nodeRadius(node.kind) + 6}
                  y={4}
                  className="fill-slate-700 text-[11px] font-medium"
                  style={{ paintOrder: "stroke", stroke: "rgba(248,250,252,0.9)", strokeWidth: 3 }}
                >
                  {node.label}
                </text>
                <title>{`${node.kind}: ${node.label}`}</title>
              </g>
            ))}
          </svg>
        )}
      </div>
      <div className="mt-4 flex flex-wrap gap-2 text-xs text-slate-600">
        <span className="rounded-full bg-slate-900 px-3 py-1 text-white">Ministro</span>
        <span className="rounded-full bg-marinho-800 px-3 py-1 text-white">Empresa</span>
        <span className="rounded-full bg-ouro-700 px-3 py-1 text-white">Parte</span>
        <span className="rounded-full bg-verde-700 px-3 py-1 text-white">Advogado</span>
        <span className="rounded-full bg-ouro-900 px-3 py-1 text-white">Representante</span>
      </div>
    </section>
  );
}
