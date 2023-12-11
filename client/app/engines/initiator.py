import base64
from PIL import Image
from io import BytesIO
from app.engines.image_captioning import ImageCaptioning
from app.engines.conv_chain import QaChainManager


chain = QaChainManager()


def generate_html_links(links):
    html_output = "<ul style='font-size: 10px;'>\n"

    for link in links:
        html_output += f"<li><a href='{link}' target='_blank'>{link}</a></li>\n"

    html_output += "</ul>"

    return html_output


class Initiate:
    def __init__(self, message):
        self.message = message

    def start(self):
        # Image handler
        if self.message.startswith("data:image/jpeg;base64,"):
            base64_string = self.message
            base64_data = base64_string.split(',')[1] if ',' in base64_string else base64_string

            image_data = base64.b64decode(base64_data)
            image = Image.open(BytesIO(image_data))
            image.save('images/decoded_image.jpg', 'JPEG')
            
            ic = ImageCaptioning()
            self.message = "tell me about " + ic.generate_caption(image_path='images/decoded_image.jpg')

        # Text handler
        answer_sources = chain.get_answer_with_sources(self.message)

        html_result = generate_html_links(answer_sources['sources'][0:3])

        return "<div style='text-align: left'>" + answer_sources['answer'] + "<br/><br/> References: <br/>" + html_result + "</div>"
    