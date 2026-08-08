use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

const MAX_RESOURCE_FILE: u64 = 256 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resource {
    pub name: String,
    pub description: String,
    pub path: PathBuf,
}

pub fn discover_skills(root: Option<&Path>, cwd: &Path) -> Result<Vec<Resource>> {
    discover(root, cwd, "skills", true)
}

pub fn discover_prompts(root: Option<&Path>, cwd: &Path) -> Result<Vec<Resource>> {
    discover(root, cwd, "prompts", false)
}

fn discover(root: Option<&Path>, cwd: &Path, kind: &str, skill: bool) -> Result<Vec<Resource>> {
    let mut directories = Vec::new();
    if let Some(root) = root {
        directories.push(root.join(kind));
    }
    let ancestors = project_ancestors(cwd);
    directories.extend(
        ancestors
            .into_iter()
            .map(|path| path.join(".agents").join(kind)),
    );

    let mut resources = Vec::new();
    for directory in directories {
        if !directory.is_dir() {
            continue;
        }
        collect(&directory, skill, &mut resources)?;
    }
    // Later, more project-specific resources override broader/global ones.
    let mut unique = std::collections::BTreeMap::new();
    for resource in resources {
        unique.insert(resource.name.clone(), resource);
    }
    Ok(unique.into_values().collect())
}

pub(crate) fn project_ancestors(cwd: &Path) -> Vec<&Path> {
    let nearest_repository = cwd.ancestors().find(|path| path.join(".git").exists());
    let mut ancestors = cwd
        .ancestors()
        .take_while(|path| nearest_repository.is_none_or(|root| path.starts_with(root)))
        .collect::<Vec<_>>();
    ancestors.reverse();
    ancestors
}

fn collect(directory: &Path, skill: bool, resources: &mut Vec<Resource>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            let manifest = path.join("SKILL.md");
            if skill && manifest.is_file() {
                resources.push(parse(&manifest, true)?);
            } else {
                collect(&path, skill, resources)?;
            }
        } else if !skill && path.extension().and_then(|value| value.to_str()) == Some("md") {
            resources.push(parse(&path, false)?);
        }
    }
    Ok(())
}

fn parse(path: &Path, require_description: bool) -> Result<Resource> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > MAX_RESOURCE_FILE {
        bail!(
            "resource must be a regular file no larger than 256 KiB: {}",
            path.display()
        );
    }
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut name = (if require_description {
        path.parent().and_then(Path::file_name)
    } else {
        path.file_stem()
    })
    .and_then(|value| value.to_str())
    .unwrap_or_default()
    .to_owned();
    let mut description = String::new();
    if let Some(frontmatter) = content
        .strip_prefix("---\n")
        .and_then(|rest| rest.split_once("\n---"))
    {
        for line in frontmatter.0.lines() {
            if let Some(value) = line.strip_prefix("name:") {
                name = value.trim().trim_matches('"').to_owned();
            }
            if let Some(value) = line.strip_prefix("description:") {
                description = value.trim().trim_matches('"').to_owned();
            }
        }
    }
    if name.is_empty() || (require_description && description.is_empty()) {
        bail!(
            "resource needs name and description frontmatter: {}",
            path.display()
        );
    }
    Ok(Resource {
        name,
        description,
        path: path.to_path_buf(),
    })
}

pub fn prompt_context(root: Option<&Path>, cwd: &Path) -> Result<String> {
    let skills = discover_skills(root, cwd)?;
    let prompts = discover_prompts(root, cwd)?;
    let mut output = String::new();
    if !skills.is_empty() {
        output.push_str("\n\n<available_skills>\n");
        output.push_str("Skills contain specialized instructions. Read the matching SKILL.md before using it.\n");
        for item in skills {
            output.push_str(&format!(
                "<skill name={:?} description={:?} path={:?} />\n",
                item.name, item.description, item.path
            ));
        }
        output.push_str("</available_skills>");
    }
    if !prompts.is_empty() {
        output.push_str("\n\n<available_prompts>\n");
        output.push_str("When a user message begins /name, read the matching prompt file, substitute $ARGUMENTS with the remaining text, and follow the expanded prompt.\n");
        for item in prompts {
            output.push_str(&format!(
                "<prompt name={:?} description={:?} path={:?} />\n",
                item.name, item.description, item.path
            ));
        }
        output.push_str("</available_prompts>");
    }
    Ok(output)
}

pub fn read_prompt(root: Option<&Path>, cwd: &Path, name: &str) -> Result<Option<String>> {
    let prompt = discover_prompts(root, cwd)?
        .into_iter()
        .find(|prompt| prompt.name == name);
    prompt
        .map(|prompt| fs::read_to_string(prompt.path).map_err(Into::into))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovers_global_and_standard_project_resources_with_nearest_override() {
        let root = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let cwd = project.path().join("src");
        fs::create_dir_all(root.path().join("skills/review")).unwrap();
        fs::write(
            root.path().join("skills/review/SKILL.md"),
            "---\nname: review\ndescription: global\n---\n",
        )
        .unwrap();
        fs::create_dir_all(project.path().join(".agents/skills/review")).unwrap();
        fs::write(
            project.path().join(".agents/skills/review/SKILL.md"),
            "---\nname: review\ndescription: project\n---\n",
        )
        .unwrap();
        fs::create_dir_all(project.path().join(".agents/prompts")).unwrap();
        fs::create_dir_all(&cwd).unwrap();
        fs::write(
            project.path().join(".agents/prompts/check.md"),
            "---\nname: check\ndescription: Check it\n---\n$ARGUMENTS",
        )
        .unwrap();
        let skills = discover_skills(Some(root.path()), &cwd).unwrap();
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].description, "project");
        assert!(
            prompt_context(Some(root.path()), &cwd)
                .unwrap()
                .contains("check.md")
        );
        assert!(
            read_prompt(Some(root.path()), &cwd, "check")
                .unwrap()
                .unwrap()
                .contains("$ARGUMENTS")
        );
    }
}
