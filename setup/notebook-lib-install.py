# Databricks notebook source
pip install Faker==15.3.4

# COMMAND ----------

def display_slide(slide_id, slide_number):
	displayHTML(f'''
	<div style="width:1750px; margin:auto">
	<iframe
		src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}" 
		frameborder="0" 
		width="1150" 
		height="683"
	></iframe></div>
	''')
