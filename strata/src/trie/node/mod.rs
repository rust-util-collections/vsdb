mod codec;
use super::{
    nibbles::Nibbles,
    shared::{Handle, TreeNode},
};
pub use codec::NodeCodec;

pub(crate) type NodeHandle = Handle<Node>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Children {
    pub bitmap: u16,
    pub nodes: Box<[NodeHandle]>,
}
impl Children {
    fn rank(&self, i: usize) -> usize {
        (self.bitmap & ((1 << i) - 1)).count_ones() as usize
    }
    pub fn get(&self, i: usize) -> Option<&NodeHandle> {
        (self.bitmap & (1 << i) != 0).then(|| &self.nodes[self.rank(i)])
    }
    pub fn get_mut(&mut self, i: usize) -> Option<&mut NodeHandle> {
        let rank = self.rank(i);
        (self.bitmap & (1 << i) != 0).then(|| &mut self.nodes[rank])
    }
    pub fn set(&mut self, i: usize, value: Option<NodeHandle>) {
        let rank = self.rank(i);
        if self.bitmap & (1 << i) != 0 {
            if let Some(new) = value {
                self.nodes[rank] = new;
                return;
            }
            self.bitmap &= !(1 << i);
            let mut nodes = std::mem::take(&mut self.nodes).into_vec();
            nodes.remove(rank);
            self.nodes = nodes.into_boxed_slice();
        } else if let Some(new) = value {
            self.bitmap |= 1 << i;
            let mut nodes = std::mem::take(&mut self.nodes).into_vec();
            nodes.insert(rank, new);
            self.nodes = nodes.into_boxed_slice();
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum Node {
    #[default]
    Null,
    Leaf {
        path: Nibbles,
        value: Vec<u8>,
    },
    Extension {
        path: Nibbles,
        child: NodeHandle,
    },
    Branch {
        children: Children,
        value: Option<Box<[u8]>>,
    },
}
impl TreeNode for Node {
    fn children<'a>(&'a self, mut visit: impl FnMut(&'a NodeHandle)) {
        match self {
            Self::Extension { child, .. } => visit(child),
            Self::Branch { children, .. } => children.nodes.iter().for_each(visit),
            _ => {}
        }
    }
    fn into_children(self, pending: &mut Vec<NodeHandle>) {
        match self {
            Self::Extension { child, .. } => pending.push(child),
            Self::Branch { children, .. } => pending.extend(children.nodes),
            _ => {}
        }
    }
    fn digest(&self) -> [u8; 32] {
        NodeCodec::hash(self)
    }
}
