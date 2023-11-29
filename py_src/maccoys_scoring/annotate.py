import asyncio
import logging

import pandas as pd
import requests
import concurrent.futures


async def process_file(input_file: str, api_url: str):
    comet_rev = ""
    with open(input_file, "r") as f:
        comet_rev = next(f)

    df = pd.read_csv(
        input_file,
        sep="\t",
        header=1,
    )

    domains_column_idx = len(df.columns)
    df.insert(domains_column_idx, column="domains", value="None")
    df.insert(domains_column_idx + 1, column="species", value="None")

    to_drop = []

    def _hit(i, row):
        is_target = not row["protein"].startswith("moy")
        if is_target and row["exp_score"] <= 0.01:
            req_url = "{}/peptides/{}".format(api_url, row["plain_peptide"])

            while True:
                try:
                    r = requests.get(req_url)

                    if r.status_code == 404:
                        to_drop.append(i)
                        break

                    res = r.json()
                    domain_names = set(map(lambda x: x["name"], res["domains"]))

                    if len(domain_names) == 0:
                        to_drop.append(i)
                        break

                    df.loc[i, "domains"] = ",".join(domain_names)
                    df.loc[i, "species"] = ",".join(
                        set(map(lambda x: str(x), res["taxonomy_ids"]))
                    )

                    logging.info("%s", ",".join(domain_names))

                    break
                except Exception as exception:
                    logging.info("%s: exception %s", req_url, exception)
        else:
            to_drop.append(i)

    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(executor, _hit, i, row) for i, row in df.iterrows()
        ]

        for response in await asyncio.gather(*tasks):
            pass

    df = df.drop(to_drop)

    with open(input_file, "w") as f:
        f.write(comet_rev)
        df.to_csv(f, sep="\t", index=False)
