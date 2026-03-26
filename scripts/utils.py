"""
Utility functions for config, RBAC, and secrets management.
"""
import json

def load_key_vault():
    with open('config/key_vault.json') as f:
        return json.load(f)['secrets']

def load_rbac():
    with open('config/rbac.json') as f:
        return json.load(f)

def check_permission(user, action):
    rbac = load_rbac()
    role = rbac['users'].get(user)
    if not role:
        return False
    return action in rbac['roles'].get(role, [])
