"""클라우드 게시기(GitHub Actions): 컴퓨터가 꺼져 있어도 예약 시각에 인스타 릴스·스레드를 게시한다.

입력  publisher/queue.json  — 로컬 pipeline/cloud_sync.py push 가 올린 승인 건(영상·커버는 이 저장소 raw 주소)
기록  publisher/state.json   — 이 스크립트만 쓴다(게시 상태·ID). 로컬은 cloud_sync.py pull 로 읽어 간다.
토큰  환경변수 IG_ACCESS_TOKEN, THREADS_ACCESS_TOKEN (GitHub Secrets). 출력하지 않는다.

모드: run(기본, 약 4분간 30초마다 확인) / once / dry(무엇을 할지 출력만) / selftest(토큰 점검만, 게시 없음)
안전장치(로컬 sns_publish.py·threads_publish.py 와 같은 규칙):
  - 슬롯이 지났고 3시간 안인 건만 게시(그 이상 지난 건 건너뜀), 한 건은 한 번만 게시
  - 게시 직전에 state 에 posting 을 먼저 커밋해 두어, 실행이 끊겨도 중복 게시하지 않는다(posting 이면 사람이 확인)
  - 릴스: 캡션에 블로그 글 주소, 커버·커버확인 필수 / 스레드: 글자수·자리표시자 점검, 링크 답글은 글 주소 HTTP 200 확인 뒤에만
"""
import datetime, email.utils, json, os, re, subprocess, sys, time, urllib.error, urllib.parse, urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUEUE, STATE = HERE / "queue.json", HERE / "state.json"
KST = datetime.timezone(datetime.timedelta(hours=9))
LATE = datetime.timedelta(hours=3)
IG_ID = "17841409643534808"
IG_GRAPH, TH_GRAPH = "https://graph.facebook.com/v21.0", "https://graph.threads.net/v1.0"
MAX_LEN, DAILY_CAP = 500, 20
PLACEHOLDER = re.compile(r"\(P-\d+[^)]*번[^)]*\)|\(.*글 번호.*\)")
UA = {"User-Agent": "Mozilla/5.0"}
MODE = (sys.argv[1] if len(sys.argv) > 1 else "run")
RUN_SECONDS, POLL = 240, 30
STAGE = {"publishing": False}   # *_publish 호출이 시작된 뒤 실패하면 중복 위험 때문에 자동 재시도하지 않는다
fail_until = {}   # (id, 단계) -> 다음 시도 가능 시각(epoch), 실패 후 5분 쉬기


class Fail(Exception):
    pass


def now():
    return datetime.datetime.now(KST)


def log(msg):
    print(f"[{now():%m-%d %H:%M:%S}] {msg}", flush=True)


def load(p, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def api(base, token_env, method, path, **params):
    params["access_token"] = os.environ.get(token_env, "")
    if not params["access_token"]:
        raise Fail(f"{token_env} 시크릿이 없습니다")
    data = urllib.parse.urlencode(params).encode()
    url = f"{base}/{path}"
    req = urllib.request.Request(url + ("" if method == "POST" else "?" + data.decode()),
                                 data=data if method == "POST" else None, method=method)
    try:
        return json.load(urllib.request.urlopen(req, timeout=90))
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read().decode()).get("error", {}).get("message", "")
        except Exception:
            msg = ""
        raise Fail(f"API 오류 {e.code}: {msg[:200]}")


def ig(method, path, **p):
    return api(IG_GRAPH, "IG_ACCESS_TOKEN", method, path, **p)


def th(method, path, **p):
    return api(TH_GRAPH, "THREADS_ACCESS_TOKEN", method, path, **p)


# ---------- 상태 저장(게시 직전·직후 즉시 커밋) ----------
def sh(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True, cwd=HERE.parent)


def commit_state(state, msg):
    STATE.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    sh("git", "add", "publisher/state.json")
    if sh("git", "diff", "--cached", "--quiet").returncode == 0:
        return
    sh("git", "commit", "-m", msg)
    for _ in range(4):
        if sh("git", "push").returncode == 0:
            return
        sh("git", "pull", "--rebase", "-X", "theirs")
        time.sleep(2)
    raise Fail("state.json 푸시 실패(중복 방지를 위해 중단)")


# ---------- 인스타 릴스 ----------
def ig_wait(cid, tries=80):
    for _ in range(tries):
        st = ig("GET", cid, fields="status_code").get("status_code")
        if st == "FINISHED":
            return
        if st in ("ERROR", "EXPIRED"):
            raise Fail(f"컨테이너 상태 {st}")
        time.sleep(3)
    raise Fail("컨테이너 준비 시간 초과")


def check_reel(it):
    if not re.search(r"blog\.naver\.com/todaystrend/\d+", it.get("caption", "")):
        raise Fail("캡션에 블로그 글 주소(blog.naver.com/todaystrend/글번호) 없음")
    if not it.get("video_url") or not it.get("cover_url"):
        raise Fail("영상/커버 주소 없음")
    if not it.get("cover_checked"):
        raise Fail("커버 미확인")


def publish_reel(pid, it, state):
    check_reel(it)
    used = ig("GET", f"{IG_ID}/content_publishing_limit", fields="quota_usage")["data"][0]["quota_usage"]
    if used >= 20:
        raise Fail(f"24시간 게시량 {used}건")
    st = state.setdefault(pid, {})
    st.update(status="posting", started_at=f"{now():%Y-%m-%d %H:%M:%S}")
    commit_state(state, f"{pid} posting")
    c = ig("POST", f"{IG_ID}/media", media_type="REELS", video_url=it["video_url"], caption=it["caption"],
           share_to_feed="true", cover_url=it["cover_url"])["id"]
    ig_wait(c)
    STAGE["publishing"] = True
    mid = ig("POST", f"{IG_ID}/media_publish", creation_id=c)["id"]
    st.update(status="published", media_id=mid, published_at=f"{now():%Y-%m-%d %H:%M:%S}")
    commit_state(state, f"{pid} published")
    log(f"{pid}: 릴스 게시 완료 {mid}")
    if it.get("pinned_comment"):
        try:
            ig("POST", f"{mid}/comments", message=it["pinned_comment"])
            st["ig_comment"] = "posted"
        except Fail as e:
            st["ig_comment"] = f"failed: {e}"
        commit_state(state, f"{pid} comment")


# ---------- 스레드 ----------
def th_wait(cid, tries=20):
    for _ in range(tries):
        s = th("GET", cid, fields="status").get("status")
        if s == "FINISHED":
            return
        if s in ("ERROR", "EXPIRED"):
            raise Fail(f"컨테이너 상태 {s}")
        time.sleep(3)
    raise Fail("컨테이너 준비 시간 초과")


def post_text(uid, text, reply_to=None):
    p = {"media_type": "TEXT", "text": text}
    if reply_to:
        p["reply_to_id"] = reply_to
    cid = th("POST", f"{uid}/threads", **p)["id"]
    th_wait(cid)
    STAGE["publishing"] = True
    return th("POST", f"{uid}/threads_publish", creation_id=cid)["id"]


def check_thread(it):
    for i, t in enumerate([it.get("text", "")] + list(it.get("replies", []))):
        if not t.strip():
            raise Fail("비어 있는 글")
        if len(t) > MAX_LEN:
            raise Fail(f"{len(t)}자 > {MAX_LEN}자")
        if PLACEHOLDER.search(t):
            raise Fail("블로그 주소 자리표시자가 남아 있음")


def url_ok(url):
    try:
        return urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30).status == 200
    except Exception:
        return False


def find_link(src):
    if src.get("url"):
        return src["url"] if url_ok(src["url"]) else None
    xml = urllib.request.urlopen(urllib.request.Request(src["rss"], headers=UA), timeout=30).read().decode("utf-8", "ignore")
    nb = None
    if src.get("not_before"):
        nb = datetime.datetime.strptime(src["not_before"], "%Y-%m-%d %H:%M").replace(tzinfo=KST)
    for m in re.finditer(r"<item>(.*?)</item>", xml, re.S):
        body = m.group(1)
        t = re.search(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", body, re.S)
        l = re.search(r"<link>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</link>", body, re.S)
        d = re.search(r"<pubDate>(.*?)</pubDate>", body, re.S)
        if nb is not None:
            try:
                if not d or email.utils.parsedate_to_datetime(d.group(1).strip()) < nb:
                    continue
            except Exception:
                continue
        if t and l and src["keyword"] in t.group(1):
            link = l.group(1).strip().split("?")[0]
            return link if url_ok(link) else None
    return None


def publish_thread(pid, it, state):
    check_thread(it)
    uid = th("GET", "me", fields="id")["id"]
    used = th("GET", f"{uid}/threads_publishing_limit", fields="quota_usage").get("data", [{}])[0].get("quota_usage", 0)
    if used >= DAILY_CAP:
        raise Fail(f"24시간 게시량 {used}건")
    st = state.setdefault(pid, {})
    st.update(status="posting", started_at=f"{now():%Y-%m-%d %H:%M:%S}")
    commit_state(state, f"{pid} posting")
    root = post_text(uid, it["text"])
    reply_ids, parent = [], root
    for r in it.get("replies", []):
        parent = post_text(uid, r, reply_to=parent)
        reply_ids.append(parent)
    st.update(status="published", media_id=root, reply_ids=reply_ids, published_at=f"{now():%Y-%m-%d %H:%M:%S}")
    commit_state(state, f"{pid} published")
    log(f"{pid}: 스레드 게시 완료 {root}")
    fu = it.get("followup")
    if fu and fu.get("delay_sec") is not None:
        time.sleep(min(int(fu["delay_sec"]), 60))
        run_followup(pid, it, state)


def run_followup(pid, it, state):
    fu, st = it["followup"], state[pid]
    if st.get("followup_done") or st.get("followup_state") == "posting" or st.get("status") != "published":
        return
    link = None
    try:
        link = find_link(fu["link_source"])
    except Exception as e:
        log(f"{pid}: 링크 조회 오류 {repr(e)[:80]}")
    if not link:
        st["followup_error"] = f"{now():%H:%M} 링크 못 찾음(글 발행 전이거나 HTTP 200 아님), 재시도 대기"
        fail_until[(pid, "followup")] = time.time() + 120
        return
    uid = th("GET", "me", fields="id")["id"]
    st["followup_state"] = "posting"
    commit_state(state, f"{pid} followup posting")
    rid = post_text(uid, fu["text"].replace("{url}", link), reply_to=st["media_id"])
    st.pop("followup_state", None)
    st.pop("followup_error", None)
    st.update(followup_done=True, followup_reply_id=rid, followup_link=link, followup_at=f"{now():%Y-%m-%d %H:%M:%S}")
    commit_state(state, f"{pid} followup done")
    log(f"{pid}: 링크 답글 게시 완료 {rid}")


# ---------- 한 번 점검 ----------
def tick():
    queue, state = load(QUEUE, {}), load(STATE, {})
    n = now()
    items = sorted(((v["slot"], k, v) for k, v in queue.items() if v.get("approved") and v.get("slot")))
    for slot_s, pid, it in items:
        slot = datetime.datetime.strptime(slot_s, "%Y-%m-%d %H:%M").replace(tzinfo=KST)
        st = state.get(pid, {})
        if st.get("status") == "published":
            fu = it.get("followup")
            if (it["kind"] == "thread" and fu and not st.get("followup_done") and n - slot <= LATE
                    and fu.get("delay_sec") is None and time.time() >= fail_until.get((pid, "followup"), 0)):
                due = datetime.datetime.strptime(st["published_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST) \
                    + datetime.timedelta(minutes=int(fu.get("delay_min", 10)))
                if n >= due:
                    guarded(pid, "followup", lambda: run_followup(pid, it, state), state)
            elif (it["kind"] == "thread" and fu and not st.get("followup_done") and n - slot <= LATE
                  and fu.get("delay_sec") is not None and time.time() >= fail_until.get((pid, "followup"), 0)
                  and st.get("followup_state") != "posting"):
                guarded(pid, "followup", lambda: run_followup(pid, it, state), state)   # 본문 직후 시도가 링크 미발견으로 실패했을 때 재시도
            continue
        if st.get("status") == "posting":
            continue   # 이전 실행이 끊김: 중복 방지를 위해 건드리지 않음(사람이 확인)
        if slot > n:
            continue
        if n - slot > LATE:
            if st.get("status") != "skipped":
                state.setdefault(pid, {})["status"] = "skipped"
                state[pid]["note"] = "슬롯 3시간 초과"
                commit_state(state, f"{pid} skipped(late)")
                log(f"{pid}: 슬롯 {slot_s} 3시간 초과, 건너뜀")
            continue
        if time.time() < fail_until.get((pid, "publish"), 0):
            continue
        if MODE == "dry":
            log(f"[dry] 게시 대상: {pid} ({it['kind']}) 슬롯 {slot_s}")
            continue
        fn = publish_reel if it["kind"] == "reel" else publish_thread
        guarded(pid, "publish", lambda: fn(pid, it, state), state)


def guarded(pid, step, fn, state):
    STAGE["publishing"] = False
    try:
        fn()
    except Fail as e:
        log(f"{pid}: {step} 실패 — {e}")
        s = state.setdefault(pid, {})
        s["last_error"] = f"{now():%m-%d %H:%M} {step}: {str(e)[:160]}"
        if s.get("status") == "posting" and not STAGE["publishing"]:
            s["status"] = "new"   # 게시 호출 전에 실패한 경우(검사·한도·컨테이너 오류)만 재시도
        elif s.get("status") == "posting":
            log(f"{pid}: 게시 호출 중 실패 — 중복 방지를 위해 posting 으로 둠(스레드·인스타에서 확인 필요)")
        commit_state(state, f"{pid} error")
        fail_until[(pid, step)] = time.time() + 300
    except Exception as e:
        log(f"{pid}: {step} 예외 {repr(e)[:160]}")
        fail_until[(pid, step)] = time.time() + 300


def selftest():
    ok = True
    for name, fn in (("인스타", lambda: ig("GET", IG_ID, fields="username")), ("스레드", lambda: th("GET", "me", fields="id,username"))):
        try:
            r = fn()
            log(f"{name} 토큰 정상: {r.get('username', r.get('id'))}")
        except Fail as e:
            ok = False
            log(f"{name} 토큰 오류: {e}")
    q = load(QUEUE, {})
    log(f"큐 {len(q)}건, 상태 {len(load(STATE, {}))}건")
    for k, v in sorted(q.items(), key=lambda kv: kv[1].get('slot', '')):
        if v.get("kind") == "reel":
            for f in ("video_url", "cover_url"):
                try:
                    code = urllib.request.urlopen(urllib.request.Request(v[f], headers=UA, method="HEAD"), timeout=30).status
                except Exception as e:
                    code = repr(e)[:40]
                if code != 200:
                    ok = False
                    log(f"{k}: {f} 접근 불가 {code}")
    sys.exit(0 if ok else 1)


def main():
    if MODE == "selftest":
        selftest()
    end = time.time() + (0 if MODE in ("once", "dry") else RUN_SECONDS)
    while True:
        try:
            tick()
        except Exception as e:
            log(f"오류 {repr(e)[:160]}")
        if time.time() >= end:
            break
        time.sleep(POLL)


if __name__ == "__main__":
    main()
