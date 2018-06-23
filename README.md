# Setup

```sh
pip install git+https://github.com/tomoemon/dsflow.git
```

# How to use

```sh
dsflow dump \
-P my-dataflow-dev \
-T gs://my-dataflow-dev.appspot.com/temp \
-S gs://my-dataflow-dev.appspot.com/staging \
-p {PROJECT_NAME} \
-n {NAMESPACE} \
-k {KIND_NAME} \
-o gs://{BUCKET}/{OUTPUT_PREFIX}
```

# Roadmap

- copy command (copy namespace, copy kinds)
- delete command
- rename command
- confirm parameters before starting
