from fastapi import HTTPException

def enforce_site_access(user, resource_site_id: int):

    # SuperAdmin can access everything
    if user["role"] == "superAdmin":
        return

    # Admins can access everything (if this is your rule)
    if user["role"] == "admin":
        return

    # Site user must match their site_id
    if user["role"] == "site":
        if user["site_id"] != resource_site_id:
            raise HTTPException(status_code=403, detail="Access denied: Not your site")

    # Any other role → deny
    return