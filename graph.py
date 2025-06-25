# proximity_dispatch/graph.py
from langgraph.graph import StateGraph, END
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from agents import load_customer_agent, compute_proximity_agent, assign_agent

class DispatchState(BaseModel):
    ticket: Dict[str, Any]
    customer: Optional[Dict[str, Any]] = None
    best: Optional[Dict[str, Any]] = None
    llm_reason: Optional[str] = None

# Build graph with only proximity flow
graph_builder = StateGraph(DispatchState)

graph_builder.add_node("load_customer", load_customer_agent)
graph_builder.add_node("compute_proximity", compute_proximity_agent)
graph_builder.add_node("assign", assign_agent)

graph_builder.set_entry_point("load_customer")
graph_builder.add_edge("load_customer", "compute_proximity")
graph_builder.add_edge("compute_proximity", "assign")
graph_builder.add_edge("assign", END)

graph = graph_builder.compile()
