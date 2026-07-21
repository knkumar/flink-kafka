import sys
import re

def patch_file(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

    # Replace longtable environment
    content = content.replace(r'\begin{longtable}', r'\begin{table*}[tbh]' + '\n' + r'\centering' + '\n' + r'\small' + '\n' + r'\setlength{\tabcolsep}{3pt}' + '\n' + r'\begin{tabular}')
    content = content.replace(r'\end{longtable}', r'\bottomrule\noalign{}' + '\n' + r'\end{tabular}' + '\n' + r'\end{table*}')
    
    # Remove longtable specific commands
    content = content.replace(r'\endhead', '')
    content = content.replace(r'\bottomrule\noalign{}' + '\n' + r'\endlastfoot', '')
    content = content.replace(r'\endlastfoot', '')
    content = content.replace(r'\endfirsthead', '')

    # Fix table width calculation for table*
    content = content.replace(r'\columnwidth', r'\textwidth')

    # Fix title
    # We match \hypertarget{...}{%\n\section{...}\label{...}}
    title_regex = re.compile(r'\\hypertarget{where-does.*?}{%\n\\section{(.*?)}\\label{.*?}}', re.DOTALL)
    match = title_regex.search(content)
    if match:
        title_text = match.group(1).replace('\n', ' ')
        content = title_regex.sub('', content)
        author_text = r'\\author{Kiran N Kumar \\\\ knkumar@iu.edu \\and Santosh K Saminathan \\\\ s13.santosh@gmail.com}'
        content = re.sub(r'(\\begin{document})', r'\\title{' + title_text + r'}\n' + author_text + r'\n\1\n\\maketitle', content)

    # Fix abstract
    # Match \hypertarget{abstract}{%\n\subsection{Abstract}\label{abstract}}
    abstract_regex = re.compile(r'\\hypertarget{abstract}{%\n\\subsection{Abstract}\\label{abstract}}(.*?)\n\n\\hypertarget{introduction}{', re.DOTALL)
    match = abstract_regex.search(content)
    if match:
        abstract_text = match.group(1).strip()
        # Replace the abstract section with \begin{abstract} ... \end{abstract}
        content = abstract_regex.sub(r'\\begin{abstract}\n' + abstract_text + r'\n\\end{abstract}\n\n\\hypertarget{introduction}{', content)

    with open(file_path, 'w') as f:
        f.write(content)

if __name__ == "__main__":
    patch_file('final_paper.tex')
