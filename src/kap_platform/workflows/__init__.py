"""Workflows: graphs of KAP agents and platform steps (router, validator, human
review, fallback, aggregator), designed in the console and run by the worker.
KAP has no workflow orchestration; each agent step is an ordinary KAP job.

  graph.py   graph semantics: readiness, routes, inputs, limits (pure)
  decide.py  router and validator decisions on the model server
  store.py   wf_workflow / wf_run / wf_step
  runner.py  queue handler wf_tick
  api.py     REST routes /api/t/{tid}/workflows, workflow-runs
"""
