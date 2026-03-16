from __future__ import annotations
import lark_oapi as lark
from lark_oapi.api.im.v1 import *
import logging
import os
import json
from src.core.utils.config_helper import ConfigHelper

logger = logging.getLogger(__name__)

class FeishuClient:
    """
    Feishu (Lark) API client wrapper using lark-oapi.
    """
    def __init__(self, bot_name: str = None):
        # Determine which bot to use (default to 'amazon_bot' or config's default_bot)
        if not bot_name:
            bot_name = ConfigHelper.get("integrations.feishu.default_bot", "amazon_bot")

        bot_config = ConfigHelper.get_feishu_bot(bot_name)

        if not bot_config:
            logger.error(f"Feishu bot '{bot_name}' not configured. Set FEISHU_{bot_name.upper()}_APP_ID in .env")
            self.app_id = ""
            self.app_secret = ""
            self.user_access_token = ""
            self.webhook_url = ""
        else:
            self.app_id = bot_config["app_id"]
            self.app_secret = bot_config["app_secret"]
            self.user_access_token = bot_config["user_access_token"]
            self.webhook_url = bot_config["webhook_url"]
        
        if not self.app_id or not self.app_secret:
            logger.warning(f"Feishu App ID/Secret missing for bot '{bot_name}'.")

        self.client = lark.Client.builder() \
            .app_id(self.app_id) \
            .app_secret(self.app_secret) \
            .log_level(lark.LogLevel.INFO) \
            .build()

    def send_text_message(self, receive_id_type: str, receive_id: str, text: str):
        """
        Send a simple text message.
        receive_id_type: 'open_id', 'user_id', 'union_id', 'email', 'chat_id'
        """
        content = json.dumps({"text": text})
        request = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(CreateMessageRequestBody.builder()
                          .receive_id(receive_id)
                          .msg_type("text")
                          .content(content)
                          .build()) \
            .build()

        response = self.client.im.v1.message.create(request)
        
        if not response.success():
            logger.error(f"Feishu send message failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
        
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def send_card_message(self, receive_id_type: str, receive_id: str, text: str):
        """
        Send a card message that can be updated later.
        """
        content = json.dumps({
            "config": {"wide_screen_mode": True},
            "elements": [
                {"tag": "markdown", "content": text}
            ]
        })
        
        request = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(CreateMessageRequestBody.builder()
                          .receive_id(receive_id)
                          .msg_type("interactive")
                          .content(content)
                          .build()) \
            .build()

        response = self.client.im.v1.message.create(request)
        
        if not response.success():
            logger.error(f"Feishu send card failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
        
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def update_card_message(self, message_id: str, text: str):
        """
        Update an existing card message.
        Note: Feishu only allows patching 'interactive' (card) messages, not raw 'text' messages.
        """
        content = json.dumps({
            "config": {"wide_screen_mode": True},
            "elements": [
                {"tag": "markdown", "content": text}
            ]
        })
        
        request = PatchMessageRequest.builder() \
            .message_id(message_id) \
            .request_body(PatchMessageRequestBody.builder()
                          .content(content)
                          .build()) \
            .build()

        response = self.client.im.v1.message.patch(request)

        if not response.success():
            logger.error(f"Feishu update card failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}

        # PatchMessageResponse has no data attribute, just confirm success
        return {"success": True}

    def get_chat_list(self, page_size: int = 20):
        """
        Get list of chats (groups) the bot is in.
        """
        request = ListChatRequest.builder() \
            .page_size(page_size) \
            .build()

        response = self.client.im.v1.chat.list(request)
        
        if not response.success():
            logger.error(f"Feishu get chat list failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "items": lark.JSON.marshal(response.data.items)}

    def send_webhook_message(self, webhook_url: str, text: str):
        """
        Send a text message via Feishu Webhook URL.
        """
        import requests
        payload = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }
        try:
            response = requests.post(webhook_url, json=payload)
            response.raise_for_status()
            return {"success": True, "data": response.json()}
        except Exception as e:
            logger.error(f"Feishu Webhook failed: {e}")
            return {"success": False, "error": str(e)}

    def list_bitable_records(self, app_token: str, table_id: str, view_id: str = None, page_size: int = 20, user_access_token: str = None):
        """
        List records from a specific Bitable table.
        """
        from lark_oapi.api.bitable.v1 import ListAppTableRecordRequest
        
        request = ListAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .view_id(view_id) \
            .page_size(page_size) \
            .build()

        # If user_access_token is provided, use it to override the default client auth
        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table_record.list(request, option)
        
        if not response.success():
            logger.error(f"Feishu list bitable records failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "items": lark.JSON.marshal(response.data.items)}

    def add_bitable_record(self, app_token: str, table_id: str, fields: dict, user_access_token: str = None):
        """
        Add a new record to a Bitable table.
        """
        from lark_oapi.api.bitable.v1 import CreateAppTableRecordRequest, AppTableRecord
        
        request = CreateAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(AppTableRecord.builder()
                          .fields(fields)
                          .build()) \
            .build()

        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table_record.create(request, option)
        
        if not response.success():
            logger.error(f"Feishu add bitable record failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def update_bitable_record(self, app_token: str, table_id: str, record_id: str, fields: dict, user_access_token: str = None):
        """
        Update an existing record in a Bitable table.
        """
        from lark_oapi.api.bitable.v1 import UpdateAppTableRecordRequest, AppTableRecord
        
        request = UpdateAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .record_id(record_id) \
            .request_body(AppTableRecord.builder()
                          .fields(fields)
                          .build()) \
            .build()

        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table_record.update(request, option)
        
        if not response.success():
            logger.error(f"Feishu update bitable record failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def get_bitable_record(self, app_token: str, table_id: str, record_id: str, user_access_token: str = None):
        """
        Get a single record from a Bitable table.
        """
        from lark_oapi.api.bitable.v1 import GetAppTableRecordRequest
        
        request = GetAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .record_id(record_id) \
            .build()

        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table_record.get(request, option)
        
        if not response.success():
            logger.error(f"Feishu get bitable record failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def delete_bitable_record(self, app_token: str, table_id: str, record_id: str, user_access_token: str = None):
        """
        Delete a record from a Bitable table.
        """
        from lark_oapi.api.bitable.v1 import DeleteAppTableRecordRequest
        
        request = DeleteAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .record_id(record_id) \
            .build()

        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table_record.delete(request, option)
        
        if not response.success():
            logger.error(f"Feishu delete bitable record failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def list_bitable_tables(self, app_token: str, user_access_token: str = None):
        """
        List all tables within a Bitable.
        """
        from lark_oapi.api.bitable.v1 import ListAppTableRequest
        
        request = ListAppTableRequest.builder() \
            .app_token(app_token) \
            .build()

        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table.list(request, option)
        
        if not response.success():
            logger.error(f"Feishu list bitable tables failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "items": lark.JSON.marshal(response.data.items)}

    def copy_bitable(self, app_token: str, name: str, folder_token: str = None, user_access_token: str = None):
        """
        Copy an existing Bitable.
        """
        from lark_oapi.api.bitable.v1 import CopyAppRequest, App
        
        request = CopyAppRequest.builder() \
            .app_token(app_token) \
            .request_body(App.builder()
                          .name(name)
                          .folder_token(folder_token)
                          .build()) \
            .build()

        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app.copy(request, option)
        
        if not response.success():
            logger.error(f"Feishu copy bitable failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def create_bitable(self, name: str, folder_token: str = None, user_access_token: str = None):
        """
        Create a new Bitable.
        """
        from lark_oapi.api.bitable.v1 import CreateAppRequest, App
        
        request = CreateAppRequest.builder() \
            .request_body(App.builder()
                          .name(name)
                          .folder_token(folder_token)
                          .build()) \
            .build()

        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app.create(request, option)
        
        if not response.success():
            logger.error(f"Feishu create bitable failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def create_bitable_field(self, app_token: str, table_id: str, field_name: str, field_type: int = 1, user_access_token: str = None):
        """
        Create a new field (column) in a Bitable table.
        field_type: 1 for Text, 2 for Number, etc.
        """
        from lark_oapi.api.bitable.v1 import CreateAppTableFieldRequest, AppTableField
        
        request = CreateAppTableFieldRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(AppTableField.builder()
                          .field_name(field_name)
                          .type(field_type)
                          .build()) \
            .build()

        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table_field.create(request, option)
        
        if not response.success():
            # If error is 1254401 (duplicate field name), we can ignore it
            if response.code != 1254401:
                logger.error(f"Feishu create field '{field_name}' failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
            
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def batch_add_bitable_records(self, app_token: str, table_id: str, records_list: list[dict], user_access_token: str = None):
        """
        Add multiple records to a Bitable table in one batch.
        """
        from lark_oapi.api.bitable.v1 import BatchCreateAppTableRecordRequest, BatchCreateAppTableRecordRequestBody, AppTableRecord
        
        # Split into chunks of 100 (Feishu's limit for batch create)
        chunk_size = 100
        results = []
        
        for i in range(0, len(records_list), chunk_size):
            chunk = records_list[i:i + chunk_size]
            records = [AppTableRecord.builder().fields(fields).build() for fields in chunk]
            
            request = BatchCreateAppTableRecordRequest.builder() \
                .app_token(app_token) \
                .table_id(table_id) \
                .request_body(BatchCreateAppTableRecordRequestBody.builder()
                              .records(records)
                              .build()) \
                .build()

            option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
            response = self.client.bitable.v1.app_table_record.batch_create(request, option)
            
            if not response.success():
                logger.error(f"Feishu batch add records failed: {response.code}, {response.msg}")
                results.append({"success": False, "error": response.msg, "code": response.code})
            else:
                results.append({"success": True, "data": lark.JSON.marshal(response.data)})
                
        return results

    def delete_all_bitable_records(self, app_token: str, table_id: str, user_access_token: str = None):
        """
        Fetch all records in a table and delete them to clear the table.
        Used to remove default empty rows in a new Bitable.
        """
        records_res = self.list_bitable_records(app_token, table_id, user_access_token=user_access_token)
        if records_res.get("success"):
            items = json.loads(records_res["items"])
            if items:
                from lark_oapi.api.bitable.v1 import BatchDeleteAppTableRecordRequest, BatchDeleteAppTableRecordRequestBody
                record_ids = [item["record_id"] for item in items]
                
                # Split into chunks of 100 for batch delete
                chunk_size = 100
                for i in range(0, len(record_ids), chunk_size):
                    chunk = record_ids[i:i + chunk_size]
                    request = BatchDeleteAppTableRecordRequest.builder() \
                        .app_token(app_token) \
                        .table_id(table_id) \
                        .request_body(BatchDeleteAppTableRecordRequestBody.builder()
                                      .records(chunk)
                                      .build()) \
                        .build()
                    
                    option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
                    self.client.bitable.v1.app_table_record.batch_delete(request, option)
                    logger.info(f"Deleted {len(chunk)} existing rows from table {table_id}")

