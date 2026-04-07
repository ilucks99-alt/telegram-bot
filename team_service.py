import json
import os
import config


def load_team_members() -> dict:
    if not os.path.exists(config.TEAM_MEMBER_FILE):
        return {}

    with open(config.TEAM_MEMBER_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_team_members(data: dict) -> None:
    with open(config.TEAM_MEMBER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def register_team_member(chat_id: int, name: str) -> None:
    team = load_team_members()
    team[name.strip()] = str(chat_id)
    save_team_members(team)


def find_team_member_chat_id(name: str) -> str | None:
    team = load_team_members()
    return team.get(name.strip())