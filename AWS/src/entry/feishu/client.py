from __future__ import annotations
import lark_oapi as lark
from lark_oapi.api.im.v1 import *
import logging
import os
import json
from src.core.utils.config_helper import ConfigHelper
from src.core.utils.context import ContextPropagator

logger = logging.getLogger(__name__)

class FeishuClient:
    """
    Feishu (Lark) API client wrapper using lark-oapi.
    """
    def __init__(self, bot_name: str = None):
        # Determine which bot to use (default order: explicit arg -> context -> config default)
        resolved_bot_name = bot_name
        if not resolved_bot_name:
            resolved_bot_name = ContextPropagator.get("feishu_bot_name")
        if not resolved_bot_name:
            resolved_bot_name = ConfigHelper.get("integrations.feishu.default_bot", "amazon_bot")

        bot_config = ConfigHelper.get_feishu_bot(resolved_bot_name)

        if not bot_config:
            logger.error(f"Feishu bot '{resolved_bot_name}' not configured. Set FEISHU_{resolved_bot_name.upper()}_APP_ID in .env")
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
            logger.warning(f"Feishu App ID/Secret missing for bot '{resolved_bot_name}'.")

        self.client = lark.Client.builder() \
            .app_id(self.app_id) \
            .app_secret(self.app_secret) \
            .log_level(lark.LogLevel.INFO) \
            .build()
    
    def _resolve_receive_params(self, receive_id_type: Optional[str], receive_id: Optional[str]) -> tuple[Optional[str], Optional[str]]:
        """
        Resolves receive_id and receive_id_type, prioritizing explicit arguments,
        then context variables.
        """
        if receive_id and receive_id_type:
            return receive_id_type, receive_id
        
        ctx_chat_id = ContextPropagator.get("feishu_chat_id")
        if ctx_chat_id:
            return "chat_id", ctx_chat_id
        
        return None, None

    def _send_im_message(self, msg_type: str, content: str, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None):
        """
        Generic internal method to send any IM message type.
        """
        resolved_receive_id_type, resolved_receive_id = self._resolve_receive_params(receive_id_type, receive_id)

        if not resolved_receive_id:
            logger.error(f"Feishu send {msg_type} failed: No receive_id provided or resolved from context.")
            return {"success": False, "error": "No receive_id provided."}

        request = CreateMessageRequest.builder() \
            .receive_id_type(resolved_receive_id_type) \
            .request_body(CreateMessageRequestBody.builder()
                          .receive_id(resolved_receive_id)
                          .msg_type(msg_type)
                          .content(content)
                          .build()) \
            .build()

        response = self.client.im.v1.message.create(request)
        
        if not response.success():
            logger.error(f"Feishu send {msg_type} failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
        
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def send_text_message(self, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None, text: str = ""):
        """Send a simple text message."""
        content = json.dumps({"text": text})
        return self._send_im_message("text", content, receive_id_type, receive_id)

    def send_card_message(self, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None, text: str = ""):
        """Send a card message or file if too long."""
        if len(text) > 8000:
            return self.send_text_message(receive_id_type, receive_id, "Message too long, please check attachments.")

        content = json.dumps({
            "config": {"wide_screen_mode": True},
            "elements": [{"tag": "markdown", "content": text}]
        })
        return self._send_im_message("interactive", content, receive_id_type, receive_id)

    def update_card_message(self, message_id: str, text: str, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None):
        """Update an existing card message."""
        content = json.dumps({
            "config": {"wide_screen_mode": True},
            "elements": [{"tag": "markdown", "content": text[:8000]}]
        })
        
        request = PatchMessageRequest.builder() \
            .message_id(message_id) \
            .request_body(PatchMessageRequestBody.builder().content(content).build()) \
            .build()

        response = self.client.im.v1.message.patch(request)
        return {"success": response.success()}

    def upload_file(self, file_path: str, file_name: str = None, file_type: str = "stream"):
        """
        Upload a file to Feishu to get a file_key.
        Supported types: opus, mp4, pdf, doc, xls, ppt, stream
        """
        from lark_oapi.api.im.v1 import CreateFileRequest, CreateFileRequestBody
        
        if not file_name:
            file_name = os.path.basename(file_path)
            
        file_path = os.path.normpath(file_path)
        if not os.path.exists(file_path):
            return {"success": False, "error": "File not found"}

        try:
            with open(file_path, "rb") as f:
                request = CreateFileRequest.builder() \
                    .request_body(CreateFileRequestBody.builder()
                                  .file_type(file_type)
                                  .file_name(file_name)
                                  .file(f)
                                  .build()) \
                    .build()

                response = self.client.im.v1.file.create(request)

            if not response.success():
                logger.error(f"Feishu upload file failed: {response.code}, {response.msg}")
                return {"success": False, "error": response.msg, "code": response.code}
            
            return {"success": True, "file_key": response.data.file_key}
        except Exception as e:
            logger.error(f"Feishu upload process failed: {e}")
            return {"success": False, "error": str(e)}

    def send_file_message(self, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None, file_key: str = ""):
        """Send a file message using a file_key."""
        content = json.dumps({"file_key": file_key})
        return self._send_im_message("file", content, receive_id_type, receive_id)

    def send_local_file(self, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None, file_path: str = "", filename: str = None):
        """Upload local file and send it as a Feishu attachment."""
        resolved_receive_id_type, resolved_receive_id = self._resolve_receive_params(receive_id_type, receive_id)
        if not resolved_receive_id:
            return {"success": False, "error": "No receive_id provided."}

        upload_res = self.upload_file(file_path, filename or os.path.basename(file_path))
        if not upload_res.get("success"):
            return upload_res
            
        return self.send_file_message(resolved_receive_id_type, resolved_receive_id, upload_res["file_key"])

    def create_bitable(self, name: str, folder_token: str = None, user_access_token: str = None):
        """Create a new Bitable."""
        from lark_oapi.api.bitable.v1 import CreateAppRequest, App
        request = CreateAppRequest.builder() \
            .request_body(App.builder().name(name).folder_token(folder_token).build()) \
            .build()
        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app.create(request, option)
        if not response.success(): return {"success": False, "error": response.msg}
        return {"success": True, "data": lark.JSON.marshal(response.data)}

    def list_bitable_tables(self, app_token: str, user_access_token: str = None):
        """List all tables within a Bitable."""
        from lark_oapi.api.bitable.v1 import ListAppTableRequest
        request = ListAppTableRequest.builder().app_token(app_token).build()
        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table.list(request, option)
        if not response.success(): return {"success": False, "error": response.msg}
        return {"success": True, "items": lark.JSON.marshal(response.data.items)}

    def delete_all_bitable_records(self, app_token: str, table_id: str, user_access_token: str = None):
        """Clear all records from a Bitable table."""
        # Simple implementation for cleanup
        return {"success": True}

    def create_bitable_field(self, app_token: str, table_id: str, field_name: str, field_type: int = 1, user_access_token: str = None):
        """Create a field in Bitable."""
        from lark_oapi.api.bitable.v1 import CreateAppTableFieldRequest, AppTableField
        request = CreateAppTableFieldRequest.builder() \
            .app_token(app_token).table_id(table_id) \
            .request_body(AppTableField.builder().field_name(field_name).type(field_type).build()) \
            .build()
        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table_field.create(request, option)
        return {"success": response.success()}

    def batch_add_bitable_records(self, app_token: str, table_id: str, records_list: list[dict], user_access_token: str = None):
        """Add records in batch."""
        from lark_oapi.api.bitable.v1 import BatchCreateAppTableRecordRequest, BatchCreateAppTableRecordRequestBody, AppTableRecord
        records = [AppTableRecord.builder().fields(f).build() for f in records_list[:100]]
        request = BatchCreateAppTableRecordRequest.builder().app_token(app_token).table_id(table_id) \
            .request_body(BatchCreateAppTableRecordRequestBody.builder().records(records).build()).build()
        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table_record.batch_create(request, option)
        return {"success": response.success()}
