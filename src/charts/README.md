# Archery Kubernetes Helm Deployment Guide

> [!IMPORTANT]  
> This Helm chart is no longer recommended for active use. It is kept for historical reasons only.
> Please read the [wiki](https://github.com/hhyo/Archery/wiki/k8s) for the latest deployment guide.

## 1. Install Dependencies

helm dependency update

## 2. Replace MySQL, Redis, and Archery Login Passwords

### 2.1 mysql
Run the command below, replacing `${your mysql password}` with your target MySQL password:
`grep -rn "MYSQL_ROOT_PASSWORD" *|awk -F: '{print $1}'|uniq|xargs sed -i s/MYSQL_ROOT_PASSWORD/${your mysql password}/g`

### 2.2 redis
Run the command below, replacing `${your redis password}` with your target Redis password:
`grep -rn "REDIS_PASSWORD" *|awk -F: '{print $1}'|uniq|xargs sed -i s/REDIS_PASSWORD/${your redis password}/g`

### 2.3 Default Archery Admin Password
Run the command below, replacing `${your archery password}` with your target Archery password:
`grep -rn "ARCHERY_ADMIN_PASSWORD" *|awk -F: '{print $1}'|uniq|xargs sed -i s/ARCHERY_ADMIN_PASSWORD/${your archery password}/g`

## 3. Change MySQL Persistence Configuration

For MySQL storage persistence, configure it using the options in `values.yaml`.

## 4. LDAP Settings

To enable LDAP, modify the related settings in `settings.py` under `configMap` in `values.yaml`.

## 5. Access Methods

5.1 Local access: `kubectl port-forward pods/archery-xxxxxx 9123:9123`  
5.2 External cluster access: configure `svc` as `NodePort` or `LoadBalancer`, or enable `ingress`.

Default username: `admin`  
Password: the value configured in section 2.3
