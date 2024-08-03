from __future__ import with_statement
from AnalyticsClient import AnalyticsClient
import pandas as pd
import streamlit as st
from streamlit.components.v1 import html
from reportlab.lib.pagesizes import landscape, letter, portrait
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph
from reportlab.lib import colors
import requests
from PIL import Image
from io import BytesIO
import pikepdf
import os
import time
import re
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

def zohoExport(viewid):
    class Config:
        CLIENTID = "1000.DQ32DWGNGDO7CV0V1S1CB3QFRAI72K"
        CLIENTSECRET = "92dfbbbe8c2743295e9331286d90da900375b2b66c"
        REFRESHTOKEN = "1000.0cd324af15278b51d3fc85ed80ca5c04.7f4492eb09c6ae494a728cd9213b53ce"
        PROPERTYFILEPATH = 'tokens.properties';
        ENCRYPTIONKEY = b'GiP-M-5xC76uR2on5q8TF8Hk-TkroydAPst3Qzy0syw=';
        ORGID = "60006357703"
        VIEWID = viewid
        WORKSPACEID = "174857000004732522"

    class sample:
        ac = AnalyticsClient(Config.CLIENTID, Config.CLIENTSECRET, Config.REFRESHTOKEN, Config.PROPERTYFILEPATH, Config.ENCRYPTIONKEY)

        def export_data(self, ac):
            response_format = "csv"
            file_path_template = "PDFReport_{}.csv"
            bulk = ac.get_bulk_instance(Config.ORGID, Config.WORKSPACEID)

            for view_id in view_ids:
                file_path = file_path_template.format(view_id)
                bulk.export_data(view_id, response_format, file_path)
    try:
        obj = sample()
        view_ids = [viewid]
        obj.export_data(obj.ac)

    except Exception as e:
        print(str(e))

    return 'Data Export'

def generate_catalogue_pdf(Platform, BrandName, subcategory, price_range, productcount, UTM, UTMSource, UTMCampaign, UTMMedium, format, SellerName, Aging, df, option, progress_callback=None): 
    def compress_pdf(input_pdf_path, output_pdf_path):
        if not os.path.exists(input_pdf_path):
            raise FileNotFoundError(f"Input PDF file '{input_pdf_path}' does not exist.")
        with pikepdf.open(input_pdf_path) as pdf:
            pdf.save(output_pdf_path, compress_streams=True)

    def sort_dataframe_by_variant_count(df):
        subcategory_counts = df.groupby('SubCategory')['variantid'].count().reset_index()
        subcategory_counts.columns = ['SubCategory', 'Count']
        sorted_subcategories = subcategory_counts.sort_values(by='Count', ascending=False)['SubCategory']
        df['SubCategory'] = pd.Categorical(df['SubCategory'], categories=sorted_subcategories, ordered=True)
        return df.sort_values('SubCategory')
    
    def parse_colors(color_text):
        color_mapping = {
            'red': '#ff0000', 'blue': '#0000ff', 'green': '#008000', 'yellow': '#ffff00', 
            'black': '#000000', 'white': '#ffffff', 'brown': '#a52a2a', 'orange': '#ffa500', 
            'pink': '#ffc0cb', 'purple': '#800080', 'gray': '#808080', 'grey': '#808080', 
            'multicolor': None, 'military': '#2e3b4e'
        }
        colors_list = re.findall(r'[a-zA-Z]+', color_text.lower())
        color_codes = [color_mapping.get(color) for color in colors_list if color in color_mapping]
        return color_codes or ['#0000FF']

    def create_pdf(df, output_file, max_image_width, max_image_height, format, orientation='portrait' ):
        if orientation == 'portrait':
            page_width, page_height = portrait(letter)
        elif orientation == 'landscape':
            page_width, page_height = landscape(letter)
        else:
            raise ValueError("Invalid orientation. Please specify 'portrait' or 'landscape'.")

        if format == '4x5':
            num_columns, num_rows, max_image_width, max_image_height = 4, 5, 146, 175
            page_width, page_height  = 685, 1040 
            margin_rows, margin_columns = 10, 20
        else:
            num_columns, num_rows, max_image_width, max_image_height  = 2, 3, 146 + 400, 175 + 400
            page_width, page_height = 685 + 420, (1040 + 400) * 1.5
            margin_rows, margin_columns = 100, 30
       
        total_width = (max_image_width + margin_columns) * num_columns + margin_columns * (num_columns - 1) + margin_rows * 2
        total_height = (max_image_height + margin_rows) * num_rows + margin_rows * 2
        x_offset = (page_width - total_width) / 2 + 25
        y_offset = (page_height - total_height) / 2

        c = canvas.Canvas(output_file, pagesize=(page_width, page_height))
        styles = getSampleStyleSheet()
        small_image_path = "BijnisLogo.png"
        small_image_width, small_image_height = 140, 70

        subcategories = df['SubCategory'].unique()
        total_steps = len(subcategories)
        step, start_time = 0, time.time()

        for subcategory in subcategories:
            subcategory_df = df[df['SubCategory'] == subcategory]
            pages = (len(subcategory_df) + num_columns * num_rows - 1) // (num_columns * num_rows)

            for page in range(pages):
                c.drawImage(small_image_path, (page_width + margin_rows) - small_image_width - 100, page_height - small_image_height, width=small_image_width, height=small_image_height)
                subcategory_upper = f"{subcategory.upper()}"
                c.setFont("Helvetica-Bold", 40)
                text_width = c.stringWidth(subcategory_upper, 'Helvetica-Bold', 40)
                c.drawString((page_width / 2 - text_width / 2), page_height - 40, subcategory_upper)
            
                sub_df = subcategory_df.iloc[page * num_columns * num_rows:(page + 1) * num_columns * num_rows]
                image_urls = sub_df['App_Image'].astype(str).tolist()
                product_names = sub_df['ProductName'].astype(str).tolist()
                price_ranges = sub_df['Price_Range'].astype(str).tolist()
                deeplink_urls = sub_df['App_Deeplink'].astype(str).tolist()
                sizes = sub_df['VariantSize'].astype(str).tolist()
                colors_list = sub_df['Color'].astype(str).tolist()

                page_has_content = False
                for i, (image_url, product_name, price_range, deeplink_url, size, color_text) in enumerate(zip(image_urls, product_names, price_ranges, deeplink_urls, sizes, colors_list)):
                    row_index = i // num_columns
                    col_index = i % num_columns
                    x = x_offset + margin_columns + col_index * (max_image_width + margin_columns) + 50
                    y = y_offset + margin_rows + (num_rows - row_index - 1) * (max_image_height + margin_rows)

                    response = requests.get(image_url)
                    if response.status_code == 200:
                        img_bytes = BytesIO(response.content)
                        img = Image.open(img_bytes)
                        img.thumbnail((max_image_width, max_image_height - 30))
                        c.drawImage(ImageReader(img_bytes), x, y + 60, width=max_image_width, height=max_image_height - 80, preserveAspectRatio=True)
                        c.linkURL(deeplink_url, (x, y + 60, x + max_image_width, y + max_image_height - 80))

                        rect_color = colors.HexColor("#F26522")
                        c.setStrokeColor(rect_color)
                        c.setLineWidth(4)
                        c.roundRect(x, y - 30, max_image_width + 20, max_image_height + 90, radius=10)

                        hyperlink_style = ParagraphStyle('hyperlink', parent=styles['BodyText'], fontName='Helvetica-Bold', fontSize=24, textColor=colors.black, underline=True)
                        product_info = f'{product_name}<br/><br/><font size="18" color="red" style="text-decoration: underline; text-decoration-thickness: 2px;">CLICK </font><font size="18" color="black" style="text-decoration: underline; text-decoration-thickness: 2px;"> For More Info</font>'
                        hyperlink = f'<a href="{deeplink_url}" color="black">{product_info}</a>'
                        p = Paragraph(hyperlink, hyperlink_style)

                        if len(product_name) <= 20:
                            hyperlink_style.fontSize = 30
                            p = Paragraph(hyperlink, hyperlink_style)
                            pwidth = c.stringWidth(product_name, 'Helvetica-Bold', 30)
                        else:
                            hyperlink_style.fontSize = 600 / len(product_name)
                            p = Paragraph(hyperlink, hyperlink_style)
                            pwidth = c.stringWidth(product_name, 'Helvetica-Bold', 600 / len(product_name))

                        p.wrapOn(c, max_image_width, max_image_height)
                        p.drawOn(c, x + (((max_image_width) / 2) - (pwidth / 2)), y + 590)

                        c.setStrokeColor('black')
                        c.setLineWidth(2)
                        c.setFillColor('black')
                        c.roundRect(x + (((max_image_width) / 2) - (pwidth / 2)) - 5, y + 565, pwidth + 40, 60, radius=10)
                        p.wrapOn(c, max_image_width, max_image_height)
                        p.drawOn(c, x + (((max_image_width) / 2) - (pwidth / 2)), y + 590)

                        color_codes = parse_colors(color_text)
                        rect_y = y + 30
                        rect_width = 25
                        rect_height = 25
                        rect_spacing = 5
                        c.setFont("Helvetica", 18)

                        for j, color_code in enumerate(color_codes):
                            rect_x = x + j * (rect_width + rect_spacing) + 120
                            c.setFillColor(color_code)
                            c.setLineWidth(0)
                            c.rect(rect_x, rect_y - 40, rect_width, rect_height, fill=1)
                            break

                        if len(size) <= 20:
                            c.setFont("Helvetica", 28)
                            c.setFillColor('black')
                            c.drawString(rect_x + 50, rect_y - 40, f"Size: {size}")
                        else:
                            c.setFont("Helvetica", 12)
                            c.setFillColor('black')
                            c.drawString(rect_x + 50, rect_y - 40, f"Size: {size}")

                        c.setFillColor('yellow')
                        c.circle(x + 500, rect_y + 80, 70, fill=1, stroke=1)
                        c.setFont("Helvetica-Bold", 20)
                        c.setFillColor('black')
                        c.drawString(x + 450, rect_y + 80, 'Price (Rs):')

                        if len(price_range) >= 11:
                            c.setFont("Helvetica-Bold", 22)
                            c.drawString(x + 450, rect_y + 60, f'{price_range}')
                        else:
                            c.setFont("Helvetica-Bold", 24)
                            c.drawString(x + 450, rect_y + 60, f'{price_range}')

                        page_has_content = True
                    else:
                        print(f"Failed to download image from {image_url}")

                if page_has_content:
                    c.showPage()

            step += 1
            elapsed_time = time.time() - start_time
            avg_time_per_step = elapsed_time / step
            remaining_steps = total_steps - step
            estimated_time_remaining = avg_time_per_step * remaining_steps
            if progress_callback:
                progress_callback(step / total_steps, estimated_time_remaining)

        c.save()

    output_file = 'sample_catalogue.pdf'
    compressed_output_file = 'sample_catalogue_compressed.pdf'
    max_image_width = 146 + 400
    max_image_height = 175 + 400
    print(df.head())
    df['SubCategory'] = df['SubCategory'].fillna('')
    print(BrandName)

    if Platform != "All":
        df = df[df['Platform'] == Platform]
    if BrandName != ['All']:
        df = df[df['BrandName'].isin(BrandName)]
        df['SubCategory'] = df['BrandName']
    if subcategory != "All":
        df = df[df['SubCategory'] == subcategory]
    if SellerName != "All":
        df = df[df['SellerName'] == SellerName]
    if price_range is not None:
        df = df[(df['Avg_Price'] >= price_range[0]) & (df['Avg_Price'] <= price_range[1])]


    if option == "New Launched Variants":
        print("New Launched Variants")
        if Aging is not None:
            df = df[(df['Aging'] >= Aging[0]) & (df['Aging'] <= Aging[1])]
            # df = df[df['Aging']==Aging[1]]

    if BrandName == ["All"]:
        if option != "New Launched Variants":
            if productcount is not None:
                if SellerName != "All":
                    df = df[(df['rankSeller'] <= productcount[1])]
                elif Platform == "BijnisExpress":
                    df = df[df['rankBijExp'] <= productcount[1]]
                elif Platform == "Production":
                    df = df[df['rankPP'] <= productcount[1]]
                elif Platform == "Distribution":
                    df = df[df['rankDP'] <= productcount[1]]
                else:
                    df = df[df['rankOverall'] <= productcount[1]]

    if UTM == 'Custom':
        deeplink(UTMSource, UTMCampaign, UTMMedium, df)
        # Adding Custom UTM to the dataframe
        link_df = pd.read_csv('Postman_Deeplink_Final.csv')
        df = df.merge(link_df, how='left', on='variantid')
        df = df.rename(columns={"App_Deeplink_y": "App_Deeplink"})

    df = sort_dataframe_by_variant_count(df)
    df['Color'] = df['Color'].fillna('')
    df['Color'] = df['Color'].apply(parse_colors)

    create_pdf(df, output_file, max_image_width, max_image_height, format, orientation='portrait', )
    compress_pdf(output_file, compressed_output_file)
    print("PDF generation and compression completed.")

    return compressed_output_file

# Function to load subcategory data
def load_subcategory_data(file_path):
    subcategory_list_df = pd.read_csv(file_path)
    return subcategory_list_df

# Main application logic

def main():
    page_bg_img = '''
<style>
@import url('https://fonts.googleapis.com/css2?family=Proxima Nova:wght@700&display=swap');
[data-testid="stAppViewContainer"] {
    background: linear-gradient(to right, #FFCC99, #C2E0FF);
    background-size: cover;
h1 {
    font-family: 'Proxima Nova', sans-serif;
}
}
</style>
'''
    st.markdown(page_bg_img, unsafe_allow_html=True)
    st.title("The PDF Tool")
    
    
    #Loading Data for Top Performaing Variants
    subcategory_list_df = pd.read_csv('PDFReport_174857000100873355.csv')
    #Loading Data for New Launched Variants
    new_df = pd.read_csv('PDFReport_174857000103979557.csv')
    
    # Session state to keep track of submission state
    if 'submitted' not in st.session_state:
        st.session_state.submitted = False
    
    # Select Box Logic
    option = st.selectbox(
        "Select the required report:",
        ["Top Performing Variants", "New Launched Variants"],
        index=0,
        key='report_select'
    )

    # Handle the Filter button
    if st.button('Filter'):
        st.session_state.submitted = True

    if st.session_state.submitted:
        if option == "Top Performing Variants":
            Platform, BrandName, subcategory, price_ranges, productcount, UTM, UTMSource, UTMCampaign, UTMMedium, format, SellerName = handle_top_performing_variants(subcategory_list_df)
            Aging = None
            if st.button('Process', key='download_button'):
                viewid = "174857000100873355"
                zohoExport(viewid)
                df = pd.read_csv('PDFReport_174857000100873355.csv')
                progress_bar = st.progress(0)
                progress_text = st.empty()

                def update_progress(progress, estimated_time_remaining):
                    progress_bar.progress(progress)
                    progress_text.text(f"Estimated time remaining: {int(estimated_time_remaining)} seconds")
                
                result = generate_catalogue_pdf(Platform, BrandName, subcategory, price_ranges, productcount, UTM, UTMSource, UTMCampaign, UTMMedium, format, SellerName, Aging, df, option,  update_progress)
                with open(result, "rb") as pdf_file:
                    st.download_button(
                        label="Download PDF",
                        data=pdf_file,
                        file_name="TopSellingProducts.pdf",
                        mime="application/pdf"
                )
            
        elif option == "New Launched Variants":
            Platform, BrandName, subcategory, price_ranges, UTM, UTMSource, UTMCampaign, UTMMedium, format, SellerName, Aging  = handle_yesterday_launched_variants(new_df)
            if st.button('Process', key='download_button'):
                viewid = "174857000103979557"
                zohoExport(viewid)
                df = pd.read_csv('PDFReport_174857000103979557.csv')
                progress_bar = st.progress(0)
                progress_text = st.empty()
                def update_progress(progress, estimated_time_remaining):
                    progress_bar.progress(progress)
                    progress_text.text(f"Estimated time remaining: {int(estimated_time_remaining)} seconds")
                productcount = None
                result = generate_catalogue_pdf(Platform, BrandName, subcategory, price_ranges, productcount, UTM, UTMSource, UTMCampaign, UTMMedium, format, SellerName, Aging, df, option,  update_progress)
                with open(result, "rb") as pdf_file:
                    st.download_button(
                        label="Download PDF",
                        data=pdf_file,
                        file_name="NewLaunchedProducts.pdf",
                        mime="application/pdf"
                )

# Function to handle UI and logic for Top Performing Variants
def handle_top_performing_variants(subcategory_list_df):
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])   
    with col1:
        Platform = st.selectbox("Select Platform", ["All", "Production", "Distribution", "BijnisExpress"], index=0)
        st.write(f"You selected: {Platform}")
        
        UTM = st.selectbox("Select UTM", ["Default", "Custom"], index=0)
        if UTM == "Custom":
            UTMSource = st.text_input("Input UTM Source")
            UTMCampaign = st.text_input("Input UTM Campaign")
            UTMMedium = st.text_input("Input UTM Medium")
        else:
            UTMSource = "BI_Campaign"
            UTMCampaign = "BI_Campaign"
            UTMMedium = "BI_Campaign"


        filtered_brand = subcategory_list_df['BrandName'].unique().tolist()
        
        brands_with_all = ["All"] + filtered_brand

# Use st.multiselect to allow multiple selections
        BrandName = st.multiselect("Select Brands", brands_with_all, default="All")

        # Handle the selection
        if "All" in BrandName:
            st.write("You selected: All brands")
        else:
            st.write(f"You selected: {', '.join(BrandName)}")
    
    with col2:
        supercategory = st.selectbox("Select SuperCat", ["All", "Footwear", "Apparels"])
        st.write(f"You selected: {supercategory}")
        
        if Platform != 'All' and supercategory != 'All':
            filtered_sellers = subcategory_list_df[(subcategory_list_df['Platform'] == Platform) & (subcategory_list_df['SuperCategory'] == supercategory)]['SellerName'].unique().tolist()
        elif Platform != 'All':
            filtered_sellers = subcategory_list_df[subcategory_list_df['Platform'] == Platform]['SellerName'].unique().tolist()
        elif supercategory != 'All':
            filtered_sellers = subcategory_list_df[subcategory_list_df['SuperCategory'] == supercategory]['SellerName'].unique().tolist()
        else:
            filtered_sellers = subcategory_list_df['SellerName'].unique().tolist()
        
        SellerName = st.selectbox("Select SellerName", ["All"] + filtered_sellers, index=0)
        st.write(f"You selected: {SellerName}")
    
    with col3:
        if SellerName != 'All':
            filtered_subcategories = subcategory_list_df[subcategory_list_df['SellerName'] == SellerName]['SubCategory'].unique().tolist()
        elif supercategory != 'All':
            filtered_subcategories = subcategory_list_df[subcategory_list_df['SuperCategory'] == supercategory]['SubCategory'].unique().tolist()
        else:
            filtered_subcategories = subcategory_list_df['SubCategory'].unique().tolist()
        
        subcategory = st.selectbox("Select Subcat", ["All"] + filtered_subcategories, index=0)
        st.write(f"You selected: {subcategory}")
        
        productcount = st.slider("Select Count", 0, 100, (0, 100), step=1)
        st.write(f"Top {productcount} Products")

    with col4:
        format = st.selectbox("Select PDF Format", ["2x3"], index=0)
        st.write(f"You selected: {format}")

        price_ranges = st.slider("Select Price Range", 0, 5000, (0, 5000), step=50)
        st.write(f"You selected: {price_ranges}")    

        return Platform, BrandName, subcategory, price_ranges, productcount, UTM, UTMSource, UTMCampaign, UTMMedium,  format,  SellerName

def handle_yesterday_launched_variants(new_df):
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    
    with col1:
        Platform = st.selectbox("Select Platform", ["All", "Production", "Distribution", "BijnisExpress"], index=0)
        st.write(f"You selected: {Platform}")
        
        UTM = st.selectbox("Select UTM", ["Default", "Custom"], index=0)
        if UTM == "Custom":
            UTMSource = st.text_input("Input UTM Source")
            UTMCampaign = st.text_input("Input UTM Campaign")
            UTMMedium = st.text_input("Input UTM Medium")
        else:
            UTMSource = "BI_Campaign"
            UTMCampaign = "BI_Campaign"
            UTMMedium = "BI_Campaign"
        
        
        filtered_brand = new_df['BrandName'].unique().tolist()
        
        brands_with_all = ["All"] + filtered_brand

# Use st.multiselect to allow multiple selections
        BrandName = st.multiselect("Select Brands", brands_with_all, default="All")

        # Handle the selection
        if "All" in BrandName:
            st.write("You selected: All brands")
        else:
            st.write(f"You selected: {', '.join(BrandName)}")
    
    with col2:
        supercategory = st.selectbox("Select SuperCat", ["All", "Footwear", "Apparels"])
        st.write(f"You selected: {supercategory}")
        
        if Platform != 'All' and supercategory != 'All':
            filtered_sellers = new_df[(new_df['Platform'] == Platform) & (new_df['SuperCategory'] == supercategory)]['SellerName'].unique().tolist()
        elif Platform != 'All':
            filtered_sellers = new_df[new_df['Platform'] == Platform]['SellerName'].unique().tolist()
        elif supercategory != 'All':
            filtered_sellers = new_df[new_df['SuperCategory'] == supercategory]['SellerName'].unique().tolist()
        else:
            filtered_sellers = new_df['SellerName'].unique().tolist()
        
        SellerName = st.selectbox("Select SellerName", ["All"] + filtered_sellers, index=0)
        st.write(f"You selected: {SellerName}")
    
    with col3:
        if SellerName != 'All':
            filtered_subcategories = new_df[new_df['SellerName'] == SellerName]['SubCategory'].unique().tolist()
        elif supercategory != 'All':
            filtered_subcategories = new_df[new_df['SuperCategory'] == supercategory]['SubCategory'].unique().tolist()
        else:
            filtered_subcategories = new_df['SubCategory'].unique().tolist()
        
        subcategory = st.selectbox("Select Subcat", ["All"] + filtered_subcategories, index=0)
        st.write(f"You selected: {subcategory}")
        
        price_ranges = st.slider("Select Price Range", 0, 5000, (0, 5000), step=50)
        st.write(f"You selected: {price_ranges}")

        with col4:
            format = st.selectbox("Select PDF Format", ["2x3", "4x5"], index=0)
            st.write(f"You selected: {format}") 

            Aging = st.slider("Select Aging", 1, 30, (1, 30), step=1)
            st.write(f"You selected: {Aging}")

        return Platform, BrandName, subcategory, price_ranges, UTM, UTMSource, UTMCampaign, UTMMedium, format, SellerName, Aging
    
def deeplink(UTMSource, UTMCampaign, UTMMedium, df):
    # Initialize logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    csv_file = 'BijnisDeeplinkPDF.csv'
    json_file = 'output.json'
    output_csv = 'Postman_Deeplink_Final.csv'

    # Save the dataframe to CSV
    df.to_csv(csv_file, index=False)
    data = pd.read_csv(csv_file)

    # URL and headers for the API request
    url = 'https://api.bijnis.com/g/ba/generate/pdplink/'
    headers = {
        'Content-Type': 'application/json'
    }
    # Initialize a list to hold all the responses
    responses = []
    # Function to handle individual requests
    def make_request(variant_id):
        # Create the payload as specified in the cURL command
        payload = {
            "nid": [variant_id],
            "utmSource": UTMSource,
            "utmCampaign": UTMCampaign,
            "utmMedium": UTMMedium
        }
        # Make the POST request
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return {'variantid': variant_id, 'response': response.json()}
        except requests.RequestException as e:
            logging.error(f"Request failed for variant_id {variant_id}: {e}")
            return {'variantid': variant_id, 'error': str(e)}
    # Use ThreadPoolExecutor to make parallel requests
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(make_request, int(row['variantid'])): int(row['variantid']) for _, row in data.iterrows()}

        # Use tqdm to display a progress bar
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching data"):
            responses.append(future.result())
    # Write all responses to a JSON file
    with open(json_file, 'w') as outfile:
        json.dump(responses, outfile, indent=4)

    logging.info("Data fetching complete. Responses saved to output.json.")
    # Verify if the JSON file was created
    if not os.path.exists(json_file):
        logging.error(f"Error: File '{json_file}' not found. Please check the path.")
        return
    # Read the JSON file and extract the required data
    final_data = []
    try:
        with open(json_file, 'r') as file:
            data = json.load(file)

            for item in data:
                variant_id = item['variantid']

                if 'response' in item and 'url' in item['response']:
                    url = item['response']['url'][str(variant_id)]
                    final_data.append({'variantid': variant_id, 'App_Deeplink': url})
                else:
                    logging.warning(f"Missing 'response' or 'url' key in item with variantid: {variant_id}")

        if final_data:
            df = pd.DataFrame(final_data)
            df.to_csv(output_csv, index=False)
            logging.info('Success! CSV file created.')
        else:
            logging.warning("No data extracted due to missing keys in JSON objects.")

    except FileNotFoundError:
        logging.error(f"Error: File '{json_file}' not found. Please check the path.")
    except json.JSONDecodeError:
        logging.error(f"Error: Could not decode JSON data from '{json_file}'.")

# Execute the main function
if __name__ == "__main__":
    main()
