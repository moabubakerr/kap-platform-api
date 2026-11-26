"""Evaluations: run a set of test questions through a KAP agent and score the answers.

  score.py   answer text from a KAP job, fact checks (numbers compared as numbers), pass rule, run summary
  judge.py   AI judge over an OpenAI-compatible model server (correctness, grounding, completeness)
  store.py   eval_set / eval_case / eval_run / eval_result
  runner.py  queue handlers eval_run / eval_watch: submit the questions as KAP jobs, follow them, score
  api.py     REST routes /api/t/{tid}/eval-sets, eval-runs
"""
