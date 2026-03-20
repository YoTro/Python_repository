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
        # Prioritize explicitly passed arguments
        if receive_id and receive_id_type:
            return receive_id_type, receive_id
        
        # Try to get from context if not explicitly provided
        ctx_chat_id = ContextPropagator.get("feishu_chat_id")
        logger.info(f"[DEBUG] _resolve_receive_params: ctx_chat_id={ctx_chat_id}, current_context={ContextPropagator.get_all()}") # Added debug log
        
        if ctx_chat_id:
            logger.debug(f"Resolved receive_id from context: {ctx_chat_id}")
            # Assume 'chat_id' type for context-resolved chat IDs
            return "chat_id", ctx_chat_id
        
        return None, None

    def _send_im_message(self, msg_type: str, content: str, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None):
        """
        Generic internal method to send any IM message type.
        """
        resolved_receive_id_type, resolved_receive_id = self._resolve_receive_params(receive_id_type, receive_id)

        if not resolved_receive_id:
            logger.error(f"Feishu send {msg_type} failed: No receive_id provided or resolved from context.")
            return {"success": False, "error": "请提供您的飞书 `open_id` 或其他 `receive_id` (例如 `email`, `user_id`)，以便我将消息发送给您。"}

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
        """
        Send a simple text message.
        """
        content = json.dumps({"text": text})
        return self._send_im_message("text", content, receive_id_type, receive_id)

    def send_card_message(self, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None, text: str = ""):
        """
        Send a card message. If the text is too long, it's sent as a file attachment instead,
        with automatic format detection (JSON or TXT).
        """
        if len(text) > 8000:  # A safe threshold for Feishu cards
            import tempfile
            import os
            
            filename = "response.txt"
            try:
                json.loads(text)
                filename = "response.json"
            except json.JSONDecodeError:
                pass  # Not JSON, default to .txt

            resolved_receive_id_type, resolved_receive_id = self._resolve_receive_params(receive_id_type, receive_id)

            if not resolved_receive_id:
                logger.error("Feishu send card as file failed: No receive_id provided or resolved from context.")
                return {"success": False, "error": "Cannot send file, receive_id is unknown."}

            self.send_text_message(resolved_receive_id_type, resolved_receive_id, f"The response is too large to display in a message card. I am sending it as a {filename} file instead.")
            
            temp_path = None
            try:
                # Use a suffix to help OS identify the file type
                with tempfile.NamedTemporaryFile(mode='w', suffix=os.path.splitext(filename)[1], delete=False, encoding='utf-8') as f:
                    f.write(text)
                    temp_path = f.name
                
                return self.send_local_file(resolved_receive_id_type, resolved_receive_id, temp_path, filename)
            finally:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)

        content = json.dumps({
            "config": {"wide_screen_mode": True},
            "elements": [
                {"tag": "markdown", "content": text}
            ]
        })
        return self._send_im_message("interactive", content, receive_id_type, receive_id)

    def update_card_message(self, message_id: str, text: str, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None):
        """
        Update an existing card message.
        If text is too long, sends it as a file and updates the card with a notification.
        Requires receive_id and receive_id_type to send the file.
        """
        if len(text) > 8000:  # Safe threshold
            import tempfile
            import os
            
            filename = "response.txt"
            try:
                json.loads(text)
                filename = "response.json"
            except json.JSONDecodeError:
                pass # Not JSON.
            
            resolved_receive_id_type, resolved_receive_id = self._resolve_receive_params(receive_id_type, receive_id)
            
            if resolved_receive_id:
                temp_path = None
                try:
                    # First, send the large content as a file
                    with tempfile.NamedTemporaryFile(mode='w', suffix=os.path.splitext(filename)[1], delete=False, encoding='utf-8') as f:
                        f.write(text)
                        temp_path = f.name
                    
                    self.send_local_file(resolved_receive_id_type, resolved_receive_id, temp_path, filename)
                    
                    # Then, update the original card to notify the user
                    text = f"The response is too large to display in a message card. I have sent it as a {filename} file instead."
                finally:
                    if temp_path and os.path.exists(temp_path):
                        os.remove(temp_path)
            else:
                # Fallback to truncation if we can't send a file
                text = text[:8000] + "\n\n... (message truncated as receiver ID was not available to send as a file)"

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

    def upload_file(self, file_path: str, file_name: str, file_type: str = "stream"):
        """
        Upload a file to Feishu to get a file_key.
        file_type: 'stream', 'all', 'pdf', 'doc', 'xls', 'ppt'
        """
        from lark_oapi.api.im.v1 import CreateFileRequest, CreateFileRequestBody
        
        file_path = os.path.normpath(file_path)
        file = open(file_path, "rb")
        request = CreateFileRequest.builder() \
            .request_body(CreateFileRequestBody.builder()
                          .file_type(file_type)
                          .file_name(file_name)
                          .file(file)
                          .build()) \
            .build()

        response = self.client.im.v1.file.create(request)
        file.close()

        if not response.success():
            logger.error(f"Feishu upload file failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code}
        
        return {"success": True, "file_key": response.data.file_key}

    def send_file_message(self, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None, file_key: str = ""):
        """
        Send a file message using a file_key.
        """
        content = json.dumps({"file_key": file_key})
        return self._send_im_message("file", content, receive_id_type, receive_id)


    def send_data_as_file(self, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None, data: list[dict] = [], filename: str = "export.csv"):
        """
        Helper: Convert list of dicts to a temporary CSV and send it.
        """
        import csv
        import tempfile
        
        resolved_receive_id_type, resolved_receive_id = self._resolve_receive_params(receive_id_type, receive_id)

        if not resolved_receive_id:
            logger.error("Feishu send data as file failed: No receive_id provided or resolved from context.")
            return {"success": False, "error": "请提供您的飞书 `open_id` 或其他 `receive_id` (例如 `email`, `user_id`)，以便我将文件发送给您。"}

        if not data:
            return {"success": False, "error": "No data to send"}

        # Create a temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            temp_path = f.name

        try:
            # Upload
            upload_res = self.upload_file(temp_path, filename, file_type="stream")
            if not upload_res.get("success"):
                return upload_res
            
            # Send
            return self.send_file_message(resolved_receive_id_type, resolved_receive_id, upload_res["file_key"])
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def send_url_as_file(self, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None, url: str = "", filename: str = "downloaded_file"):
        """
        Download a file from URL and send it as a Feishu file attachment.
        """
        import requests
        import tempfile
        
        resolved_receive_id_type, resolved_receive_id = self._resolve_receive_params(receive_id_type, receive_id)

        if not resolved_receive_id:
            logger.error("Feishu send URL as file failed: No receive_id provided or resolved from context.")
            return {"success": False, "error": "请提供您的飞书 `open_id` 或其他 `receive_id` (例如 `email`, `user_id`)，以便我将文件发送给您。"}

        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Try to guess extension from content-type or URL if not in filename
            if "." not in filename:
                ext = ".bin"
                content_type = response.headers.get('content-type', '')
                if 'spreadsheet' in content_type or 'excel' in content_type:
                    ext = ".xlsx"
                elif 'csv' in content_type:
                    ext = ".csv"
                elif 'pdf' in content_type:
                    ext = ".pdf"
                filename += ext

            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                for chunk in response.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                temp_path = tmp.name

            try:
                return self.send_local_file(resolved_receive_id_type, resolved_receive_id, temp_path, filename)
            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    
        except Exception as e:
            logger.error(f"Failed to download and send URL as file: {e}")
            return {"success": False, "error": str(e)}

    def send_local_file(self, receive_id_type: Optional[str] = None, receive_id: Optional[str] = None, file_path: str = "", filename: str = None):
        """
        Complete flow: Upload local file and send it as a Feishu attachment.
        """
        resolved_receive_id_type, resolved_receive_id = self._resolve_receive_params(receive_id_type, receive_id)

        if not resolved_receive_id:
            logger.error("Feishu send local file failed: No receive_id provided or resolved from context.")
            return {"success": False, "error": "请提供您的飞书 `open_id` 或其他 `receive_id` (例如 `email`, `user_id`)，以便我将文件发送给您。"}

        if not filename:
            filename = os.path.basename(file_path)
            
        upload_res = self.upload_file(file_path, filename)
        if not upload_res.get("success"):
            return upload_res
            
        return self.send_file_message(receive_id_type, receive_id, upload_res["file_key"])

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
        
        builder = ListAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .page_size(page_size)
        
        if view_id:
            builder.view_id(view_id)
            
        request = builder.build()

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

    def list_bitable_fields(self, app_token: str, table_id: str, user_access_token: str = None):
        """Lists all fields (columns) in a specific Bitable table."""
        from lark_oapi.api.bitable.v1 import ListAppTableFieldRequest
        
        request = ListAppTableFieldRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .page_size(100) \
            .build()
            
        option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
        response = self.client.bitable.v1.app_table_field.list(request, option)
        
        if not response.success():
            logger.error(f"Feishu list bitable fields failed: {response.code}, {response.msg}")
            return {"success": False, "error": response.msg, "code": response.code, "items": []}
            
        return {"success": True, "items": json.loads(lark.JSON.marshal(response.data.items))}

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
                if response.code == 1254405:
                    # Diagnose which field name is missing
                    field_names = list(chunk[0].keys()) if chunk else []
                    logger.error(f"FieldNameNotFound Diagnosis: Fields sent: {field_names}. "
                                 f"Check for case sensitivity, spaces, or missing fields in the target table.")
                results.append({"success": False, "error": response.msg, "code": response.code})
            else:
                results.append({"success": True, "data": lark.JSON.marshal(response.data)})
                
        return results

    def batch_update_bitable_records(self, app_token: str, table_id: str, updates: list[dict], user_access_token: str = None):
        """
        Batch update existing records.
        updates: list of {"record_id": str, "fields": dict}
        """
        from lark_oapi.api.bitable.v1 import BatchUpdateAppTableRecordRequest, BatchUpdateAppTableRecordRequestBody, AppTableRecord

        chunk_size = 100
        results = []

        for i in range(0, len(updates), chunk_size):
            chunk = updates[i:i + chunk_size]
            records = [
                AppTableRecord.builder().record_id(u["record_id"]).fields(u["fields"]).build()
                for u in chunk
            ]

            request = BatchUpdateAppTableRecordRequest.builder() \
                .app_token(app_token) \
                .table_id(table_id) \
                .request_body(BatchUpdateAppTableRecordRequestBody.builder()
                              .records(records)
                              .build()) \
                .build()

            option = lark.RequestOption.builder().user_access_token(user_access_token).build() if user_access_token else None
            response = self.client.bitable.v1.app_table_record.batch_update(request, option)

            if not response.success():
                logger.error(f"Feishu batch update records failed: {response.code}, {response.msg}")
                if response.code == 1254405:
                    # Diagnose which field name is missing
                    field_names = list(chunk[0]["fields"].keys()) if chunk else []
                    logger.error(f"FieldNameNotFound Diagnosis (Update): Fields sent: {field_names}. "
                                 f"Check for case sensitivity, spaces, or missing fields in the target table.")
                results.append({"success": False, "error": response.msg, "code": response.code})
            else:
                results.append({"success": True, "data": lark.JSON.marshal(response.data)})

        return results

    def populate_bitable_records(self, app_token: str, table_id: str, records_list: list[dict], user_access_token: str = None):
        """
        Robustly populate a Bitable. Ensures all fields exist before writing.
        First, it reuses existing empty rows (update), then appends the rest (create).
        """
        if not records_list:
            logger.warning("populate_bitable_records called with empty records_list.")
            return []

        # 1. Ensure all columns (fields) exist
        try:
            target_fields = set(records_list[0].keys())
            existing_fields_res = self.list_bitable_fields(app_token, table_id, user_access_token)
            
            if not existing_fields_res.get("success"):
                logger.error("Could not verify fields before populating, proceeding with risk.")
            else:
                existing_field_names = {item['field_name'] for item in existing_fields_res["items"]}
                missing_fields = target_fields - existing_field_names
                
                if missing_fields:
                    logger.info(f"Found missing fields in Bitable: {missing_fields}. Attempting to create them.")
                    for field_name in missing_fields:
                        # Assuming text field (type=1) is a safe default.
                        self.create_bitable_field(app_token, table_id, field_name, user_access_token=user_access_token)
        except Exception as e:
            logger.error(f"Error during pre-flight field check: {e}")

        # 2. Proceed with populating data
        results = []
        existing_res = self.list_bitable_records(app_token, table_id, page_size=100, user_access_token=user_access_token)
        existing_ids = []
        if existing_res.get("success"):
            items = json.loads(existing_res["items"]) if existing_res["items"] else []
            existing_ids = [item["record_id"] for item in items]

        # Update existing default rows
        update_count = min(len(existing_ids), len(records_list))
        if update_count > 0:
            updates = [{"record_id": existing_ids[i], "fields": records_list[i]} for i in range(update_count)]
            results.extend(self.batch_update_bitable_records(app_token, table_id, updates, user_access_token))
            logger.info(f"Updated {update_count} existing rows in table {table_id}")

        # Append remaining new records
        remaining = records_list[update_count:]
        if remaining:
            results.extend(self.batch_add_bitable_records(app_token, table_id, remaining, user_access_token))
            logger.info(f"Added {len(remaining)} new rows to table {table_id}")

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

