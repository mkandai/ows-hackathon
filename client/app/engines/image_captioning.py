from PIL import Image
from transformers import BlipProcessor, BlipForConditionalGeneration
import io


class ImageCaptioning():
	def __init__(self):
		self.processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-large")
		self.model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-large")

	def generate_caption(self, image_path):
		raw_image = Image.open(image_path).convert('RGB')

		text = "a photography of"
		inputs = self.processor(raw_image, text, return_tensors="pt")

		out = self.model.generate(**inputs)
		caption = self.processor.decode(out[0], skip_special_tokens=True)
		print(caption.replace("a photography of ", ""))
		return caption.replace("a photography of ", "")


if __name__=="__main__":
	image_captioning = ImageCaptioning()
	image_captioning.generate_caption()
