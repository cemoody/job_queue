from job_queue import JobQueue

jq  = JobQueue()


@jq.link(input_q_name="urls", output_q_name="links", batch_size=64)
def crawler(items, **env):
    links = []
    for item in items:
        url = item['url']
        links.append({'link': f"{url}/a.html", **item})
        links.append({'link': f"{url}/b.html", **item})
    return links

@jq.link(input_q_name="links", output_q_name="vecs", batch_size=128)
def transform(items, **cfg):
    vectors = []
    for item in items:
        vectors.append({'vector': [1, 2, 3], **item})
    return vectors

# Load up the DAG with initial data
urls = [dict(url=f"{idx}.com") for idx in range(256)]
jq.links['crawler'].set_inputs(urls)

# Run until all links report there's no more data
# left to process
jq.run_until_complete()

outputs = jq.links['transform'].get_outputs(1000)
print(outputs)