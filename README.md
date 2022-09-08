# IOQueue

Helps you set up multiple input and output queues and link them into a
job graph. As events flow through the queues jobs will be triggered
and automatically read and write to input and output queues.

## Installation

You can install _IOQueue_ via [pip] from [PyPI]:

```console
$ pip install io_queue
```

## Usage

```python
l = Linker(fn)

@l.link(input_q_name="urls", output_q_name="links", batch_size=batch_size)
def crawler(items, **cfg):
    logger.info(f"Processing {len(urls)} items {cfg} ")
    links = []
    for item in items:
        url = item['url']
        links.append({'link': f"{url}/a.html", **item})
        links.append({'link': f"{url}/b.html", **item})
    return links

@l.link(input_q_name="links", output_q_name="vecs", batch_size=batch_size)
def transform(items, **cfg):
    logger.info(f"Processing {len(items)} links {cfg}")
    vectors = []
    for item in items:
        vectors.append({'vector': [1, 2, 3], **item})
    return vectors

@l.link(input_q_name="vecs", output_q_name="mean_vec", batch_size=10000)
def sum_vector(items, **cfg):
    import numpy as np
    vecs = [item['vector'] for item in items]
    logger.info(f"Processing {len(vecs)} vecs {cfg}")
    if len(vecs) == 0:
        return []
    sum = float(np.concatenate(vecs).sum())
    rows = [dict(sum_vector=sum)]
    return rows

# Load up the DAG with initial data
urls = [dict(url=f"{idx}.com") for idx in range(n)]
l.links['crawler'].set_inputs(urls)

# Run until all links report there's no more data
# left to process
l.run_until_complete()
```

## License

Distributed under the terms of the [MIT license][license],
_IOQueue_ is free and open source software.
