# smart-route
The idea here is to easily route calls between services to maximize my usage limits.

Currently with a Devin sub you run out of actions not from model usage but from actually using the sandbox. They have unlimited usage for their finetuned GLM, but the issue is it's really slow so you need a cloud sandbox to make use of it. The idea for this repo is that if I have a cloud sandbox I can run requests through connecting to my Devin account for the GLM access by having calls routed to these sandbox instances with some instructions and polling. this should allow me to use swarms of cloud sandboxes quite cheaply.

Extreme example of usage:
Trial Cohere key for cheap decent LLM
Devin $20 sub for unlimited GLM per month
Devin trial account (as many as how many sandboxes necessary)

Use trial key for general orchestration and sending calls to the trial sandboxes. if rate limit hit for one trial key rotate it for another accounts trial key. Sandboxes use paid Devin model.
