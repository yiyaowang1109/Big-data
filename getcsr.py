import scrapy
import os

class GetcsrSpider(scrapy.Spider):
    name = 'getcsr'
    allowed_domains = ['adobe.com']
    start_urls = ['https://www.adobe.com/corporate-responsibility.html']

    def parse(self, response):
        # 查找所有链接到PDF的<a>标签
        pdf_links = response.css('a[href$=".pdf"]::attr(href)').getall()

        # 处理每个PDF链接
        for link in pdf_links:
            if not link.startswith('http'):
                link = response.urljoin(link)  # 补全URL

            yield {
                'file_url': link
            }

            # 下载PDF文件
            yield scrapy.Request(link, callback=self.save_pdf)

    def save_pdf(self, response):
        # 获取PDF文件的名字
        pdf_name = response.url.split('/')[-1]

        # 创建保存PDF文件的目录（如果目录不存在）
        download_dir = 'csr_reports'
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        # 将PDF文件保存到本地
        pdf_path = os.path.join(download_dir, pdf_name)

        # 防止覆盖已存在的文件
        if not os.path.exists(pdf_path):
            with open(pdf_path, 'wb') as f:
                f.write(response.body)
            self.log(f"Downloaded: {pdf_name}")
        else:
            self.log(f"File {pdf_name} already exists. Skipping download.")
