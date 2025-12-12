#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
账号数据迁移脚本
将 accounts.json 中的账号数据迁移到 SQL 数据库
"""

import os
import json
from accounts_manager import get_accounts, get_group_account_mapping
from accounts_sql_manager import get_accounts_sql_manager
from loguru import logger


def migrate_accounts():
    """迁移账号数据"""
    logger.info("开始迁移账号数据...")

    try:
        # 获取JSON中的账号数据
        json_accounts = get_accounts(mask_cookie=False)
        logger.info(f"从 JSON 读取到 {len(json_accounts)} 个账号")

        # 获取群组映射
        group_mapping = get_group_account_mapping()
        logger.info(f"从 JSON 读取到 {len(group_mapping)} 个群组映射")

        # 获取SQL管理器
        sql_manager = get_accounts_sql_manager()

        # 检查SQL中是否已有账号
        existing_accounts = sql_manager.get_accounts(mask_cookie=False)
        if existing_accounts:
            logger.warning(f"SQL 数据库中已存在 {len(existing_accounts)} 个账号")
            choice = input("是否清空现有账号并重新迁移? (y/N): ").strip().lower()
            if choice != 'y':
                logger.info("取消迁移")
                return

            # 清空现有账号
            logger.info("清空现有账号...")
            for acc in existing_accounts:
                sql_manager.delete_account(acc['id'])

        # 迁移账号
        migrated_count = 0
        id_mapping = {}  # 保存旧ID到新ID的映射

        for acc in json_accounts:
            try:
                # 添加账号到SQL
                new_acc = sql_manager.add_account(
                    cookie=acc.get('cookie', ''),
                    name=acc.get('name'),
                    make_default=acc.get('is_default', False)
                )
                id_mapping[acc['id']] = new_acc['id']
                logger.success(f"迁移账号: {acc.get('name', acc['id'])} -> {new_acc['id']}")
                migrated_count += 1
            except Exception as e:
                logger.error(f"迁移账号失败 {acc.get('name', acc['id'])}: {e}")

        logger.info(f"成功迁移 {migrated_count}/{len(json_accounts)} 个账号")

        # 迁移群组映射
        mapped_count = 0
        for group_id, old_account_id in group_mapping.items():
            try:
                # 查找新的账号ID
                new_account_id = id_mapping.get(old_account_id)
                if not new_account_id:
                    logger.warning(f"群组 {group_id} 映射的账号 {old_account_id} 未找到，跳过")
                    continue

                ok, msg = sql_manager.assign_group_account(group_id, new_account_id)
                if ok:
                    logger.success(f"迁移群组映射: group={group_id} -> account={new_account_id}")
                    mapped_count += 1
                else:
                    logger.error(f"迁移群组映射失败: {msg}")
            except Exception as e:
                logger.error(f"迁移群组映射失败 group={group_id}: {e}")

        logger.info(f"成功迁移 {mapped_count}/{len(group_mapping)} 个群组映射")

        # 备份JSON文件
        json_file = os.path.join(os.path.dirname(__file__), "accounts.json")
        if os.path.exists(json_file):
            backup_file = json_file + ".backup"
            logger.info(f"备份 JSON 文件到 {backup_file}")
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

        logger.success("迁移完成！")
        logger.info("提示：旧的 accounts.json 文件已备份为 accounts.json.backup")
        logger.info("你可以手动删除 accounts.json 文件，系统将使用新的 SQL 数据库")

    except Exception as e:
        logger.error(f"迁移失败: {e}")
        raise


if __name__ == "__main__":
    migrate_accounts()
