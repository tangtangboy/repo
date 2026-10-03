"""lake-executor 용 EC2 호스트(서울) 프로비저닝 — ARCHITECTURE.md §8.

만드는 것
  - 키페어 `lake-executor` (개인키는 deploy/lake-executor-key.pem 에 저장, gitignore)
  - 보안그룹 `lake-executor`: 22 ← 배포 PC /32, 80/443 ← 0.0.0.0/0, 아웃바운드 전체
  - t3.micro Ubuntu 24.04 amd64, 16GB gp3, Name=lake-executor
  - Elastic IP (Bybit API 키 IP 화이트리스트 대상)
결과는 deploy/aws_state.json 에 기록(gitignore). 이미 instance_id 가 있으면 중복 생성을 막기 위해 중단.

자격증명: ~/.aws/credentials. 프로필은 환경변수 AWS_PROFILE (없으면 default).
    python deploy/provision.py
    set AWS_PROFILE=myprofile && python deploy/provision.py   (Windows cmd)
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request

import boto3

REGION = "ap-northeast-2"
NAME = "lake-executor"
INSTANCE_TYPE = "t3.micro"
VOLUME_GB = 16
HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, "aws_state.json")
KEY_PATH = os.path.join(HERE, "lake-executor-key.pem")
SIGNAL_PATH = "/lake/signal"


def aws_session() -> boto3.Session:
    """AWS_PROFILE 환경변수가 있으면 그 프로필, 없으면 기본 자격증명 체인."""
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE") or None, region_name=REGION)


def my_public_ip() -> str:
    return urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()


def sslip_host(ip: str) -> str:
    """도메인이 없을 때 쓰는 기본 공개 호스트: 1.2.3.4 → 1-2-3-4.sslip.io (Let's Encrypt 발급 가능)."""
    return ip.replace(".", "-") + ".sslip.io"


def latest_ubuntu_ami(ec2) -> tuple[str, str]:
    imgs = ec2.describe_images(
        Owners=["099720109477"],  # Canonical
        Filters=[
            {"Name": "name", "Values": ["ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-amd64-server-*"]},
            {"Name": "architecture", "Values": ["x86_64"]},
            {"Name": "root-device-type", "Values": ["ebs"]},
            {"Name": "virtualization-type", "Values": ["hvm"]},
            {"Name": "state", "Values": ["available"]},
        ],
    )["Images"]
    if not imgs:
        raise SystemExit("no Ubuntu 24.04 amd64 AMI found in " + REGION)
    imgs.sort(key=lambda i: i["CreationDate"], reverse=True)
    return imgs[0]["ImageId"], imgs[0]["Name"]


def ensure_key_pair(ec2) -> None:
    """키페어를 새로 만들고 .pem 을 저장한다. 같은 이름이 AWS 에 남아 있으면 지우고 다시 만든다
    (개인키는 생성 시 한 번만 받을 수 있으므로, 로컬 .pem 이 없는 기존 키페어는 쓸모가 없다)."""
    if os.path.exists(KEY_PATH):
        raise SystemExit(f"{KEY_PATH} already exists. Remove it (and the AWS key pair '{NAME}') first "
                         "if you really want to re-provision.")
    try:
        ec2.delete_key_pair(KeyName=NAME)
    except Exception:
        pass
    kp = ec2.create_key_pair(KeyName=NAME, KeyType="rsa", KeyFormat="pem")
    with open(KEY_PATH, "w", newline="\n", encoding="utf-8") as f:
        f.write(kp["KeyMaterial"])
    try:
        os.chmod(KEY_PATH, 0o600)  # Windows 에서는 효과 없음(README 의 icacls 참고)
    except OSError:
        pass
    print("saved private key ->", KEY_PATH)


def ensure_security_group(ec2, deployer_ip: str) -> str:
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        raise SystemExit("no default VPC in " + REGION)
    vpc_id = vpcs[0]["VpcId"]
    try:
        sg_id = ec2.create_security_group(
            GroupName=NAME, Description="lake-executor: SSH from deployer, HTTP/HTTPS public", VpcId=vpc_id,
        )["GroupId"]
    except ec2.exceptions.ClientError as e:
        if "InvalidGroup.Duplicate" not in str(e):
            raise
        sg_id = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [NAME]}, {"Name": "vpc-id", "Values": [vpc_id]}]
        )["SecurityGroups"][0]["GroupId"]
    rules = [
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
         "IpRanges": [{"CidrIp": f"{deployer_ip}/32", "Description": "deploy PC SSH"}]},
        {"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
         "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "ACME HTTP challenge / redirect"}]},
        {"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
         "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "lake webhook HTTPS (Caddy)"}]},
    ]
    for rule in rules:
        try:
            ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[rule])
        except ec2.exceptions.ClientError as e:
            if "InvalidPermission.Duplicate" not in str(e):
                raise
    return sg_id


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    if os.path.exists(STATE):
        with open(STATE, encoding="utf-8") as f:
            st = json.load(f)
        if st.get("instance_id"):
            print("aws_state.json already has instance", st["instance_id"], "- aborting to avoid duplicates.")
            print(json.dumps(st, indent=2))
            return

    sess = aws_session()
    ec2 = sess.client("ec2")
    ec2r = sess.resource("ec2")
    print("aws profile:", os.environ.get("AWS_PROFILE") or "(default)", "| region:", REGION)

    deployer_ip = my_public_ip()
    print("this machine public IP:", deployer_ip)
    ami, ami_name = latest_ubuntu_ami(ec2)
    print("ubuntu AMI:", ami, ami_name)

    print("creating key pair...")
    ensure_key_pair(ec2)

    sg_id = ensure_security_group(ec2, deployer_ip)
    print("security group:", sg_id)

    print(f"launching {INSTANCE_TYPE}...")
    inst = ec2r.create_instances(
        ImageId=ami, InstanceType=INSTANCE_TYPE, KeyName=NAME,
        MinCount=1, MaxCount=1, SecurityGroupIds=[sg_id],
        BlockDeviceMappings=[{
            "DeviceName": "/dev/sda1",
            "Ebs": {"VolumeSize": VOLUME_GB, "VolumeType": "gp3", "DeleteOnTermination": True},
        }],
        MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
        TagSpecifications=[{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": NAME}]}],
    )[0]
    print("instance:", inst.id, "- waiting for running...")
    inst.wait_until_running()
    inst.reload()

    print("allocating Elastic IP...")
    eip = ec2.allocate_address(Domain="vpc", TagSpecifications=[{
        "ResourceType": "elastic-ip", "Tags": [{"Key": "Name", "Value": NAME}]}])
    ec2.associate_address(InstanceId=inst.id, AllocationId=eip["AllocationId"])
    public_ip = eip["PublicIp"]
    public_host = sslip_host(public_ip)

    state = {
        "region": REGION, "instance_id": inst.id, "sg_id": sg_id,
        "key_name": NAME, "key_path": KEY_PATH, "ami": ami,
        "allocation_id": eip["AllocationId"], "public_ip": public_ip,
        "public_host": public_host, "ssh_user": "ubuntu",
        "remote_dir": "/home/ubuntu/lake-executor",
    }
    with open(STATE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

    print("\n==== PROVISION DONE ====")
    print("ELASTIC IP            :", public_ip)
    print("default public host   :", public_host, "(sslip.io; replace with your own domain if you have one)")
    print("signal URL (for lake) :", f"https://{public_host}{SIGNAL_PATH}")
    print("ssh                   :", f"ssh -i {KEY_PATH} ubuntu@{public_ip}")
    print("state saved          ->", STATE)
    print("\nREMINDER: whitelist the Elastic IP", public_ip, "on the Bybit API key (IP restriction).")
    print("Next: python deploy/push.py   (uploads code, installs venv + Caddy; service not started yet)")


if __name__ == "__main__":
    main()
