"""AWS 계정 인벤토리: 어떤 계정인지 + EC2/Elastic IP/Lightsail/Lambda/API Gateway 를 훑고
자동매매 관련으로 보이는 리소스를 표시한다 (읽기 전용, 아무것도 바꾸지 않음).

Usage:
    python deploy/aws_inventory.py                  # 서울 + 주요 리전
    python deploy/aws_inventory.py --all-regions    # 모든 리전 (느림)

자격증명: DEPLOY_AWS_ACCESS_KEY_ID / DEPLOY_AWS_SECRET_ACCESS_KEY 환경변수 → AWS_PROFILE → 기본 체인.
"""
from __future__ import annotations

import argparse
import os
import sys

import boto3
from botocore.exceptions import BotoCoreError, ClientError

sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_REGIONS = ["ap-northeast-2", "ap-northeast-1", "ap-southeast-1", "us-east-1", "us-west-2"]
KEYWORDS = ("trade", "trading", "trader", "auto", "ath", "signal", "webhook", "lake", "bot", "bybit",
            "okx", "toobit", "binance", "copy", "quant", "zigzag", "cardnews", "edgeplan", "stayready", "coinguide")


def aws_session(region: str | None = None) -> boto3.Session:
    ak = os.environ.get("DEPLOY_AWS_ACCESS_KEY_ID")
    sk = os.environ.get("DEPLOY_AWS_SECRET_ACCESS_KEY")
    if ak and sk:
        return boto3.Session(aws_access_key_id=ak, aws_secret_access_key=sk,
                             aws_session_token=os.environ.get("DEPLOY_AWS_SESSION_TOKEN") or None,
                             region_name=region)
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE") or None, region_name=region)


def flag(text: str) -> str:
    t = (text or "").lower()
    hits = [k for k in KEYWORDS if k in t]
    return f"  <== {','.join(hits)}" if hits else ""


def name_of(tags) -> str:
    for t in tags or []:
        if t.get("Key") == "Name":
            return t.get("Value", "")
    return ""


def safe(label, fn):
    try:
        return fn()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        print(f"   ({label}: {code or 'error'} — 권한 없음 또는 미지원)")
    except BotoCoreError as e:
        print(f"   ({label}: {type(e).__name__})")
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all-regions", action="store_true")
    ap.add_argument("--regions", default=None, help="comma-separated region list")
    args = ap.parse_args()

    sess = aws_session("us-east-1")
    try:
        ident = sess.client("sts").get_caller_identity()
    except (ClientError, BotoCoreError) as e:
        print("자격증명 확인 실패:", type(e).__name__, str(e)[:200])
        return 2
    print(f"== 계정 {ident['Account']}  |  {ident['Arn']}")

    if args.regions:
        regions = [r.strip() for r in args.regions.split(",") if r.strip()]
    elif args.all_regions:
        regions = [r["RegionName"] for r in sess.client("ec2").describe_regions()["Regions"]]
    else:
        regions = DEFAULT_REGIONS

    found = 0
    for region in regions:
        s = aws_session(region)
        print(f"\n--- {region} ---")

        def ec2_list():
            nonlocal found
            ec2 = s.client("ec2")
            res = ec2.describe_instances()
            rows = []
            for r in res["Reservations"]:
                for i in r["Instances"]:
                    rows.append(i)
            for i in rows:
                nm = name_of(i.get("Tags"))
                st = i["State"]["Name"]
                ip = i.get("PublicIpAddress") or "-"
                print(f"   EC2 {i['InstanceId']} {i['InstanceType']:<10} {st:<10} ip={ip:<15} "
                      f"launched={str(i.get('LaunchTime'))[:16]} name={nm!r}{flag(nm + ' ' + str(i.get('KeyName', '')))}")
                found += 1
            addrs = ec2.describe_addresses()["Addresses"]
            for a in addrs:
                nm = name_of(a.get("Tags"))
                print(f"   EIP {a.get('PublicIp')} -> {a.get('InstanceId') or '(미연결)'} name={nm!r}{flag(nm)}")
            sgs = [g for g in ec2.describe_security_groups()["SecurityGroups"] if g["GroupName"] != "default"]
            for g in sgs:
                print(f"   SG  {g['GroupId']} {g['GroupName']!r}{flag(g['GroupName'] + ' ' + g.get('Description', ''))}")
            kps = ec2.describe_key_pairs()["KeyPairs"]
            for k in kps:
                print(f"   KEY {k['KeyName']}{flag(k['KeyName'])}")
            if not rows and not addrs and not sgs and not kps:
                print("   (EC2 리소스 없음)")
        safe("ec2", ec2_list)

        def ls_list():
            nonlocal found
            ls = s.client("lightsail")
            for i in ls.get_instances()["instances"]:
                print(f"   LIGHTSAIL {i['name']} {i.get('state', {}).get('name')} ip={i.get('publicIpAddress')} "
                      f"bundle={i.get('bundleId')}{flag(i['name'])}")
                found += 1
        if region in ("ap-northeast-2", "ap-northeast-1", "ap-southeast-1", "us-east-1", "us-west-2", "eu-west-1"):
            safe("lightsail", ls_list)

        def lambda_list():
            nonlocal found
            lam = s.client("lambda")
            fns = lam.list_functions().get("Functions", [])
            for f in fns:
                print(f"   LAMBDA {f['FunctionName']} runtime={f.get('Runtime')} modified={str(f.get('LastModified'))[:16]}{flag(f['FunctionName'])}")
                found += 1
        safe("lambda", lambda_list)

        def apigw_list():
            nonlocal found
            v2 = s.client("apigatewayv2")
            for a in v2.get_apis().get("Items", []):
                print(f"   APIGW(v2) {a.get('Name')} {a.get('ApiEndpoint')}{flag(a.get('Name', ''))}")
                found += 1
            v1 = s.client("apigateway")
            for a in v1.get_rest_apis().get("items", []):
                print(f"   APIGW(rest) {a.get('name')} id={a.get('id')} ({a.get('id')}.execute-api.{region}.amazonaws.com){flag(a.get('name', ''))}")
                found += 1
        safe("apigateway", apigw_list)

        def amplify_list():
            nonlocal found
            amp = s.client("amplify")
            for a in amp.list_apps().get("apps", []):
                print(f"   AMPLIFY {a.get('name')} id={a.get('appId')} {a.get('defaultDomain')}{flag(a.get('name', ''))}")
                found += 1
        if region in ("ap-northeast-2", "us-east-1"):
            safe("amplify", amplify_list)

    print(f"\n== 총 {found}개 컴퓨트/서버리스 리소스. '<==' 표시가 자동매매/프로젝트 키워드와 맞는 것.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
