import easyocr
from PIL import Image
import io
import re
import logging
import cv2
import numpy as np
import pdf2image
from decimal import Decimal
from datetime import datetime
import json

logger = logging.getLogger(__name__)

# Initialize EasyOCR reader (load model on startup)
reader = easyocr.Reader(['en'], gpu=False)  # English only, CPU mode

def preprocess_image(image):
    """Preprocess image for better OCR results."""
    try:
        logger.debug("Preprocessing image...")
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        else:
            gray = image
        thresh = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
        )
        denoised = cv2.fastNlMeansDenoising(thresh)
        logger.debug("Image preprocessing completed")
        return denoised
    except Exception as e:
        logger.error(f"Error preprocessing image: {str(e)}")
        return image

def extract_text(image_data, file_type):
    """Extract text from image or PDF using EasyOCR."""
    logger.info(f"Extracting text from {file_type}, size: {len(image_data)} bytes")
    try:
        if file_type == "PDF":
            logger.info("Processing PDF...")
            images = pdf2image.convert_from_bytes(image_data, dpi=300)
            full_text = ""
            for i, img in enumerate(images):
                logger.debug(f"Processing page {i+1}/{len(images)}")
                img_cv = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
                processed = preprocess_image(img_cv)
                # EasyOCR expects a numpy array (BGR format)
                result = reader.readtext(processed, detail=0)  # detail=0 returns only text
                text = "\n".join(result)
                full_text += text + "\n"
            logger.info(f"PDF extracted, characters: {len(full_text)}")
            return full_text
        else:
            logger.info("Processing image...")
            img = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)
            if img is None:
                raise ValueError("Failed to decode image")
            processed = preprocess_image(img)
            # EasyOCR expects a numpy array (BGR format)
            result = reader.readtext(processed, detail=0)  # detail=0 returns only text
            text = "\n".join(result)
            logger.info(f"Image extracted, characters: {len(text)}")
            return text
    except Exception as e:
        logger.error(f"OCR error: {str(e)}")
        return ""

def parse_receipt(text):
    """Parse receipt text to extract key fields."""
    logger.info("Parsing receipt text...")
    result = {
        "raw_text": text,
        "merchant_name": None,
        "total_amount": None,
        "receipt_date": None,
        "confidence_score": 50,  # Base confidence
        "extracted_data": {}
    }
    
    try:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        if not lines:
            return result

        # Merchant Name: Take the first non-numeric line or match "Jibs Café Bistro"
        merchant_name = None
        for line in lines[:5]:  # Check first 5 lines for header
            if not re.search(r'^\d', line) and not re.search(r'[:\d\.,]+$', line):
                merchant_name = line
                break
        if not merchant_name and "Jibs Café Bistro" in text:
            merchant_name = "Jibs Café Bistro"
        if merchant_name:
            result["merchant_name"] = merchant_name.strip()

        # Receipt Date: Match "Date: MM/DD/YYYY" or similar formats
        date_patterns = [
            r"Date\s*:\s*(\d{1,2}/\d{1,2}/\d{4})",
            r"(\d{1,2}/\d{1,2}/\d{4})"
        ]
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                date_str = match.group(1)
                for fmt in ["%m/%d/%Y", "%d/%m/%Y"]:
                    try:
                        result["receipt_date"] = datetime.strptime(date_str, fmt).date().isoformat()
                        break
                    except ValueError:
                        continue
                if result["receipt_date"]:
                    break

        # Total Amount: Match "TOTAL: KSH: 2,000.00" or similar with comma and currency
        total_patterns = [
            r"TOTAL\s*:\s*KSH\s*[\d,]+\.\d{2}",
            r"TOTAL\s*:\s*[\d,]+\.\d{2}",
            r"AMOUNT\s*[\d,]+\.\d{2}"
        ]
        for pattern in total_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                amount_str = re.search(r'[\d,]+\.\d{2}', match.group(0)).group(0).replace(',', '')
                result["total_amount"] = float(Decimal(amount_str))
                break

        # Confidence Score: Increase based on successful matches
        confidence = 50
        if result["merchant_name"]:
            confidence += 20
        if result["total_amount"]:
            confidence += 20
        if result["receipt_date"]:
            confidence += 10
        result["confidence_score"] = min(confidence, 100)  # Cap at 100

        result["extracted_data"] = result.copy()
        del result["extracted_data"]["extracted_data"]

        logger.info(f"Parsed - Merchant: {result['merchant_name']}, Amount: {result['total_amount']}, Date: {result['receipt_date']}, Confidence: {result['confidence_score']}%")
        return result
    except Exception as e:
        logger.error(f"Parse error: {str(e)}")
        return result