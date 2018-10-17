from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action

    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """

    template_fields = ('date',)
    template_ext = ('.json',)
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 http_conn_id,
                 endpoint,
                 gcs_path,
                 *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.gcs_path = gcs_path

    def execute(self, context):
        http = HttpHook(method='GET', http_conn_id=self.http_conn_id)
        response = http.run(self.endpoint)

        text_file = open("/tmp/response", "w")
        text_file.write(response.text)
        text_file.close()

        gcs = GoogleCloudStorageHook()
        gcs.upload(bucket=self.gcs_path, object=self.gcs_path, filename="/tmp/response", mime_type="application/json")
