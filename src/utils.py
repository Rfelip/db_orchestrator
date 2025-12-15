import logging
from jinja2 import Template

log = logging.getLogger(__name__)

def render_template(content, params):
    """
    Renders a Jinja2 template with the provided parameters.
    
    Args:
        content (str): The raw template string (e.g., SQL with {{ placeholders }}).
        params (dict): Dictionary of parameters to inject.
        
    Returns:
        str: The rendered string.
    """
    if not params:
        return content
        
    try:
        template = Template(content)
        return template.render(**params)
    except Exception as e:
        log.error(f"Failed to render template: {e}")
        raise