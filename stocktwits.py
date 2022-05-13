import requests
import csv
import os
import datetime

headers_json = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
    "Content-Type": "application/json",
}


def conv_time(time_, other_patern=None):
    if time_:
        if isinstance(time_, str):
            if other_patern:
                return datetime.datetime.strptime(time_, "%m-%d-%Y")
            return datetime.datetime.strptime(time_, "%Y-%m-%d")
        else:
            return time_


def save_tweet_data_to_csv(records, filepath, mode="a+"):
    header = [
        "company_ticker",
        "api_id",
        "body",
        "created_at",
        "likes",
        "likes_user_ids",
        "replies",
        "reshares_count",
        "reshares_user_ids",
        "user_id",
        "user_name",
        "name",
        "avatar_url",
        "join_date",
        "official",
        "following",
        "followers",
        "ideas",
        "watchlist_stocks_count",
        "like_count",
        "source_title",
        "source_url",
        "link_url",
        "link_image_url",
        "symbol_watchlist_count",
        "mentioned_users",
    ]
    with open(filepath, mode=mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if mode == "w":
            writer.writerow(header)
        if records:
            writer.writerow(records)


cookies = dict(
    osano_consentmanager_uuid="bb8706e2-b794-4df8-b51d-e8c640a404ef",
    osano_consentmanager="pXki7Kzoor6xS2XPvfZIWwccdUsx5Djy0KoXCXwBArvXnf3vyIxgMakGBqhNwcFEPzrira2t_CsQBQU6iueoChh2qAVtqGNJXMpSlict7yt5_LCLItbS2rdYhimWSudqrlNEsqHnYG7PPly1BmQQu9J_31Suf8gB_y65SkFKF_b-xBtEP61XxzH0bGaVBmISHuc7dmdbTxJYmePtKvn_H953aGqpOQY9G0__JlQjXJzrC2CTzFruL1r7rPPImx2cDh5OD5gfh5gaoXqUzYKsHYle49pQ6MfMJD45NA==",
    _ga="GA1.2.1524482083.1639651522",
    __qca="P0-1218954852-1639651530934",
    __ssid="361e02c8c9d614e2ec756becd97bd30",
    access_token="e7064f0ecc8792c87aab8e5506a484016d1d1929",
    auto_log_in="1",
    _pubcid="b5fba691-b3fa-40d4-99d9-7cd8625dda6f",
    _cc_id="ee765b179108339b49c903ea76fe1767",
    timezone="-180",
    _gid="GA1.2.1241058278.1640371434",
    __gads="ID=12c21d3c7794f48f:T=1640371485:S=ALNI_MZSp65q-uGGi2xreVNOVJkR86D9vQ",
    session_visits_count="4",
    session_visit_counted="true",
    cf_clearance="_1kmWE9npPaGSYVf91FrlZBI0oY2xzCSVRruAiVYWfM-1640441185-0-250",
)


def get_info(company):
    all_files = os.listdir(path="stocktwits/companies")
    exist = next((file for file in all_files if company.get("ticker") in file), None)
    if not exist:
        save_tweet_data_to_csv(
            None, "stocktwits/companies/{}.csv".format(company.get("ticker")), "w"
        )
    with open(
        "stocktwits/companies/{}.csv".format(company.get("ticker")),
        "r",
        newline="",
        encoding="utf-8",
    ) as f:
        raws = csv.DictReader(x.replace("\0", "") for x in f)

        ids_objs = sorted(
            [x for x in raws],
            key=lambda x: conv_time(x.get("created_at")[:10]),
        )
        ids = [int(x.get("api_id")) for x in ids_objs]

        count = 0
        params = {"filter": "top", "limit": 30}
        # try:
        url = (
            f"https://api.stocktwits.com/api/2/streams/symbol/{company['ticker']}.json"
        )
        response = requests.get(
            url, params=params, headers=headers_json, cookies=cookies
        )
        if not response:
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{company['new_ticker']}.json"
            response = requests.get(
                url, params=params, headers=headers_json, cookies=cookies
            )
        response_json = response.json()
        resp_info = response_json.get("cursor")
        messages = response_json.get("messages")
        more, max = (
            resp_info.get("more"),
            resp_info.get("max"),
        )
        print(more, max)
        while True:
            params["max"] = max
            response = requests.get(
                url, params=params, headers=headers_json, cookies=cookies
            )
            response_json = response.json()
            messages = response_json.get("messages")
            resp_info = response_json.get("cursor")
            more, max = (
                resp_info.get("more"),
                resp_info.get("max"),
            )
            print(more, max, count, company.get("ticker"))

            for message in messages:
                if int(message.get("id")) in ids:
                    continue
                else:
                    conversation = message.get("conversation")
                    reshares = message.get("reshares")
                    likes = message.get("likes")
                    links = message.get("links")
                    user = message.get("user")
                    object_to_write = (
                        company.get("ticker"),
                        message.get("id"),
                        message.get("body"),
                        message.get("created_at"),
                        likes.get("total") if likes else 0,
                        likes.get("user_ids") if likes else [],
                        conversation.get("replies") if conversation else 0,
                        reshares.get("reshared_count") if reshares else 0,
                        reshares.get("user_ids") if reshares else [],
                        user.get("id"),
                        user.get("username"),
                        user.get("name"),
                        user.get("avatar_url"),
                        user.get("join_date"),
                        user.get("official"),
                        user.get("following"),
                        user.get("followers"),
                        user.get("ideas"),
                        user.get("watchlist_stocks_count"),
                        user.get("like_count"),
                        message.get("source").get("title"),
                        message.get("source").get("url"),
                        [x.get("url") for x in message.get("links")] if links else None,
                        [x.get("image") for x in message.get("links") if x.get("image")]
                        if links
                        else None,
                        ",".join(
                            [
                                x.get("symbol") + "-" + str(x.get("watchlist_count"))
                                for x in message.get("symbols")
                            ]
                        ),
                        message.get("mentioned_users"),
                    )

                    save_tweet_data_to_csv(
                        object_to_write,
                        "stocktwits/companies/{}.csv".format(company.get("ticker")),
                    )
            if more is False:
                break
            count += 1


def main():
    with open("data/countries.csv", "r") as f:
        companies = csv.DictReader(f)
        companies = [x for x in companies]
        for company in companies:
            get_info(company)

if __name__ == '__main__':
    main()
